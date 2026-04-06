import ssl
import time
import logging
from contextlib import contextmanager
from typing import Optional, Tuple, List, Dict, Any
from pyVim import connect
from pyVmomi import vim

# Configurar logging
logger = logging.getLogger(__name__)

# Excepciones personalizadas
class VcenterError(Exception):
    """Error base para operaciones con vCenter"""
    pass

class VcenterConnectionError(VcenterError):
    """Error de conexión con vCenter"""
    pass

class VcenterObjectNotFound(VcenterError):
    """Objeto no encontrado en vCenter"""
    pass

class VcenterTaskError(VcenterError):
    """Error en tarea de vCenter"""
    pass

def connect_si(host: str, user: str, pwd: str, insecure: bool = True):
    """Conectar a vCenter con manejo de errores mejorado"""
    try:
        ctx = ssl._create_unverified_context() if insecure else None
        si = connect.SmartConnect(host=host, user=user, pwd=pwd, sslContext=ctx)
        if not si:
            raise VcenterConnectionError(f"No se pudo conectar a {host}")
        return si
    except Exception as e:
        raise VcenterConnectionError(f"Error conectando a {host}: {e}")

def disconnect_si(si):
    """Desconectar de vCenter con manejo silencioso"""
    try:
        connect.Disconnect(si)
    except Exception as e:
        logger.debug(f"Error al desconectar: {e}")

def get_content(si):
    return si.RetrieveContent()

def find_obj(content, vimtypes, name):
    """Buscar objeto por nombre con manejo de errores"""
    try:
        view = content.viewManager.CreateContainerView(content.rootFolder, vimtypes, True)
        try:
            for o in view.view:
                if o.name == name:
                    return o
        finally:
            view.Destroy()
    except Exception as e:
        logger.error(f"Error buscando objeto {name}: {e}")
        return None
    return None

def wait_task(task, timeout_sec: int = 1800) -> Tuple[bool, Optional[str]]:
    """
    Esperar a que una tarea de vCenter termine.
    Retorna (ok: bool, error_msg: str | None)
    """
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            st = getattr(getattr(task, "info", None), "state", None)
            if st == vim.TaskInfo.State.success:
                return True, None
            if st == vim.TaskInfo.State.error:
                err = task.info.error
                msg = getattr(err, "msg", str(err)) if err else "unknown error"
                return False, msg
        except Exception as e:
            return False, f"task check error: {e}"
        time.sleep(1)
    return False, "timeout"

def wait_for_power_state(vm, desired_state: str, timeout_sec: int) -> bool:
    """Esperar a que una VM alcance un estado de energía específico"""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            if str(vm.runtime.powerState) == desired_state:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False

def ensure_folder(datacenter, name):
    """Asegurar que existe una carpeta, crearla si no"""
    for e in datacenter.vmFolder.childEntity:
        if isinstance(e, vim.Folder) and e.name == name:
            return e
    return datacenter.vmFolder.CreateFolder(name)

def list_all_vm_folders(datacenter):
    """Listar todas las carpetas de VMs"""
    out = []
    def walk(folder):
        for e in folder.childEntity:
            if isinstance(e, vim.Folder):
                out.append(e.name)
                walk(e)
    walk(datacenter.vmFolder)
    return sorted(set(out))

def list_templates(content):
    """Listar todas las plantillas"""
    names = []
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
    try:
        for vm in view.view:
            try:
                if vm.config and vm.config.template:
                    names.append(vm.name)
            except:
                pass
    finally:
        view.Destroy()
    return sorted(set(names))

def list_hosts(content):
    """Listar todos los hosts ESXi"""
    names = []
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
    try:
        for h in view.view:
            names.append(h.name)
    finally:
        view.Destroy()
    return sorted(set(names))

def list_datastores(content):
    """Listar todos los datastores"""
    names = []
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.Datastore], True)
    try:
        for d in view.view:
            names.append(d.name)
    finally:
        view.Destroy()
    return sorted(set(names))

def list_networks(content):
    """Listar todas las redes"""
    names = []
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.Network], True)
    try:
        for n in view.view:
            names.append(n.name)
    finally:
        view.Destroy()
    return sorted(set(names))

def find_vm_in_folder(folder, name):
    """Buscar VM en una carpeta"""
    for e in folder.childEntity:
        if isinstance(e, vim.VirtualMachine) and e.name == name:
            return e
    return None

def find_vm_by_moid(content, moid: str):
    """Buscar VM por MoID"""
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
    try:
        for vm in view.view:
            if getattr(vm, "_moId", None) == moid:
                return vm
    finally:
        view.Destroy()
    return None

def build_network_device_change(vm_template, target_network):
    """Construir cambios de dispositivo para la red"""
    device_changes = []
    for dev in vm_template.config.hardware.device:
        if isinstance(dev, vim.vm.device.VirtualEthernetCard):
            nic = dev
            if isinstance(target_network, vim.dvs.DistributedVirtualPortgroup):
                backing = vim.vm.device.VirtualEthernetCard.DistributedVirtualPortBackingInfo()
                backing.port = vim.dvs.PortConnection()
                backing.port.portgroupKey = target_network.key
                backing.port.switchUuid = target_network.config.distributedVirtualSwitch.uuid
            else:
                backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
                backing.network = target_network
                backing.deviceName = target_network.name
            nic_new = vim.vm.device.VirtualDeviceSpec()
            nic_new.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
            nic_new.device = nic
            nic_new.device.backing = backing
            device_changes.append(nic_new)
    return device_changes

def clone_vm(template, target_folder, name, host, datastore, network=None, 
             power_on=False, resource_pool=None, snapshot_name=None):
    """Clonar una VM con manejo de errores mejorado"""
    relospec = vim.vm.RelocateSpec()
    if datastore:
        relospec.datastore = datastore
    if host:
        relospec.host = host
        if resource_pool is None and hasattr(host.parent, 'resourcePool'):
            resource_pool = host.parent.resourcePool
    if resource_pool:
        relospec.pool = resource_pool

    config_spec = vim.vm.ConfigSpec()
    device_changes = []
    if network:
        device_changes = build_network_device_change(template, network)
    if device_changes:
        config_spec.deviceChange = device_changes

    clonespec = vim.vm.CloneSpec(location=relospec, powerOn=power_on, 
                                  template=False, config=config_spec)

    if snapshot_name and template.snapshot:
        stack = [template.snapshot.rootSnapshotList]
        while stack:
            nodes = stack.pop()
            for node in nodes:
                if node.name == snapshot_name:
                    clonespec.snapshot = node.snapshot
                    stack = []
                    break
                if node.childSnapshotList:
                    stack.append(node.childSnapshotList)

    task = template.Clone(folder=target_folder, name=name, spec=clonespec)
    return task

def get_vm_ips(vm) -> List[str]:
    """Obtener lista de IPs de una VM de forma más robusta"""
    import ipaddress
    ips = set()
    
    def add_ip(s: str):
        if not s:
            return
        s = str(s).strip()
        if not s:
            return
        s_clean = s.split("%")[0]
        try:
            ip = ipaddress.ip_address(s_clean)
        except ValueError:
            return
        if ip.is_loopback or ip.is_link_local:
            return
        ips.add(str(ip))
    
    try:
        g = getattr(vm, "guest", None)
        if g:
            try:
                add_ip(getattr(g, "ipAddress", None))
            except Exception:
                pass
            try:
                nets = getattr(g, "net", None) or []
                for n in nets:
                    for ip in (getattr(n, "ipAddress", None) or []):
                        add_ip(ip)
            except Exception:
                pass
    except Exception:
        pass
    
    return sorted(ips)
