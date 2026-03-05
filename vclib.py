import ssl, time
from pyVim import connect
from pyVmomi import vim

def connect_si(host, user, pwd, insecure=True):
    ctx = ssl._create_unverified_context() if insecure else None
    return connect.SmartConnect(host=host, user=user, pwd=pwd, sslContext=ctx)

def disconnect_si(si):
    try:
        connect.Disconnect(si)
    except:
        pass

def get_content(si):
    return si.RetrieveContent()

def find_obj(content, vimtypes, name):
    view = content.viewManager.CreateContainerView(content.rootFolder, vimtypes, True)
    try:
        for o in view.view:
            if o.name == name:
                return o
    finally:
        view.Destroy()
    return None

def wait_tasks(si, tasks):
    pc = si.content.propertyCollector
    obj_specs = [vim.ObjectSpec(obj=t) for t in tasks]
    prop_spec = vim.PropertySpec(type=vim.Task, pathSet=[], all=True)
    filter_spec = vim.PropertyFilterSpec(objectSet=obj_specs, propSet=[prop_spec])
    tf = pc.CreateFilter(filter_spec, True)
    try:
        remaining = set(tasks)
        while remaining:
            update = pc.WaitForUpdates(None)
            for fs in update.filterSet:
                for os in fs.objectSet:
                    t = os.obj
                    info = t.info
                    if info.state in [vim.TaskInfo.State.success, vim.TaskInfo.State.error]:
                        remaining.discard(t)
                        if info.state == vim.TaskInfo.State.error:
                            raise info.error
            time.sleep(0.2)
    finally:
        try: tf.Destroy()
        except: pass

def ensure_folder(datacenter, name):
    for e in datacenter.vmFolder.childEntity:
        if isinstance(e, vim.Folder) and e.name == name:
            return e
    return datacenter.vmFolder.CreateFolder(name)

def list_all_vm_folders(datacenter):
    out = []
    def walk(folder):
        for e in folder.childEntity:
            if isinstance(e, vim.Folder):
                out.append(e.name)
                walk(e)
    walk(datacenter.vmFolder)
    return sorted(set(out))

def list_templates(content):
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
    names = []
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
    try:
        for h in view.view:
            names.append(h.name)
    finally:
        view.Destroy()
    return sorted(set(names))

def list_datastores(content):
    names = []
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.Datastore], True)
    try:
        for d in view.view:
            names.append(d.name)
    finally:
        view.Destroy()
    return sorted(set(names))

def list_networks(content):
    names = []
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.Network], True)
    try:
        for n in view.view:
            names.append(n.name)
    finally:
        view.Destroy()
    return sorted(set(names))

def find_vm_in_folder(folder, name):
    for e in folder.childEntity:
        if isinstance(e, vim.VirtualMachine) and e.name == name:
            return e
    return None

def build_network_device_change(vm_template, target_network):
    # Returns list of deviceChange mapping all NICs to target_network
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

def clone_vm(template, target_folder, name, host, datastore, network=None, power_on=False, resource_pool=None, snapshot_name=None):
    relospec = vim.vm.RelocateSpec()
    if datastore: relospec.datastore = datastore
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

    clonespec = vim.vm.CloneSpec(location=relospec, powerOn=power_on, template=False, config=config_spec)

    # Optional snapshot revert before clone (golden snapshot)
    if snapshot_name and template.snapshot:
        # Find snapshot by name
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
