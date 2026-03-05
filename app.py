#!/usr/bin/env python3
# vCenter Lab Creator con distribución por host + login simple + UI moderna (dark)
# OPTIMIZADO: ctx manager vCenter, caché TTL inventario, menos duplicación, clon más eficiente

import os
import ssl
import csv
import uuid
import time
import threading
import sqlite3
import json
from contextlib import contextmanager
from functools import wraps
from queue import Queue, Empty
from datetime import datetime

from werkzeug.security import generate_password_hash, check_password_hash
from flask import (
Flask, render_template, request, redirect, url_for,
    Response, jsonify, session, flash,
    has_request_context
)
from pyVim import connect
from pyVmomi import vim
from dotenv import load_dotenv

load_dotenv()

# ---------------- Config ----------------

def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key, str(default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)).strip())
    except Exception:
        return default


def _normalize_vcenter_cfg(vcid: str, cfg: dict, legacy_verify: bool) -> dict:
    host = (cfg.get("host") or "").strip()
    user = (cfg.get("user") or cfg.get("username") or "").strip()
    pwd  = (cfg.get("pass") or cfg.get("password") or "").strip()

    v = cfg.get("verify_ssl", legacy_verify)
    if isinstance(v, str):
        verify_ssl = v.strip().lower() in ("1","true","yes","y","on")
    else:
        verify_ssl = bool(v)

    label = (cfg.get("label") or vcid).strip() or vcid

    if not host or not user or not pwd:
        raise RuntimeError(f"Config vCenter incompleta para '{vcid}'. Requiere host/user/pass.")

    return {"id": vcid, "label": label, "host": host, "user": user, "password": pwd, "verify_ssl": verify_ssl}


def _load_vcenters_from_env():
    legacy_verify = _env_bool("VERIFY_SSL", False)
    vjson = (os.getenv("VCENTERS_JSON") or "").strip()
    if vjson:
        raw = json.loads(vjson)
        if not isinstance(raw, dict) or not raw:
            raise RuntimeError("VCENTERS_JSON debe ser un objeto JSON no vacío.")
        out = {}
        for vcid, cfg in raw.items():
            if isinstance(cfg, dict):
                out[str(vcid)] = _normalize_vcenter_cfg(str(vcid), cfg, legacy_verify)
        if not out:
            raise RuntimeError("VCENTERS_JSON no contiene entradas válidas.")
        return out

    # Legacy (1 vCenter)
    host = (os.getenv("VCENTER_HOST") or "").strip()
    user = (os.getenv("VCENTER_USER") or "").strip()
    pwd  = (os.getenv("VCENTER_PASS") or "").strip()
    if host and user and pwd:
        return {"vc1": _normalize_vcenter_cfg("vc1", {
            "label": os.getenv("VCENTER_LABEL", "vCenter"),
            "host": host,
            "user": user,
            "pass": pwd,
            "verify_ssl": legacy_verify,
        }, legacy_verify)}

    raise RuntimeError("No hay configuración de vCenter. Define VCENTERS_JSON o VCENTER_HOST/VCENTER_USER/VCENTER_PASS.")


VCENTERS = _load_vcenters_from_env()

DEFAULT_VCENTER = (os.getenv("DEFAULT_VCENTER") or "").strip() or next(iter(VCENTERS.keys()))
if DEFAULT_VCENTER not in VCENTERS:
    DEFAULT_VCENTER = next(iter(VCENTERS.keys()))

PROF_VCENTER = (os.getenv("PROF_VCENTER") or "").strip() or DEFAULT_VCENTER
if PROF_VCENTER not in VCENTERS:
    PROF_VCENTER = DEFAULT_VCENTER


def get_active_vcenter_id():
    """Devuelve el vCenter activo para la request actual. Admin puede elegir; profesor va fijo."""
    if not has_request_context():
        return DEFAULT_VCENTER

    if not session.get("authed"):
        return DEFAULT_VCENTER

    if session.get("role") == "admin":
        return session.get("vcenter_id")  # puede ser None si aún no eligió

    return session.get("vcenter_id") or PROF_VCENTER


def get_vcenter_cfg(vcenter_id=None):
    """(vcid, cfg) resolviendo defaults sin depender de request context."""
    vcid = (vcenter_id or "").strip() if vcenter_id else None
    if not vcid:
        vcid = get_active_vcenter_id() or DEFAULT_VCENTER
    if vcid not in VCENTERS:
        vcid = DEFAULT_VCENTER
    return vcid, VCENTERS[vcid]

AUTH_USER = os.getenv("AUTH_USER", "admin")
AUTH_PASS = os.getenv("AUTH_PASS", "changeme")
FLASK_SECRET = os.getenv("FLASK_SECRET", "devsecret")

USER_DB_PATH = os.getenv(
    "USER_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")
)

TMP_DIR = os.getenv("TMP_DIR", "/tmp/vclab_jobs")
os.makedirs(TMP_DIR, exist_ok=True)

LAB_FOLDER_PREFIX = os.getenv("LAB_FOLDER_PREFIX", "Lab - ")

SOFT_SHUTDOWN_TIMEOUT_SEC = _env_int("SOFT_SHUTDOWN_TIMEOUT_SEC", 60)
SOFT_SHUTDOWN_FALLBACK_HARD = _env_bool("SOFT_SHUTDOWN_FALLBACK_HARD", True)

SOFT_REBOOT_FALLBACK_RESET = _env_bool("SOFT_REBOOT_FALLBACK_RESET", True)

POLL_INTERVAL_MS = _env_int("POLL_INTERVAL_MS", 2500)

# Cachés (reduce llamadas pesadas a vCenter)
INVENTORY_CACHE_TTL_SEC = _env_int("INVENTORY_CACHE_TTL_SEC", 60)
LABS_CACHE_TTL_SEC = _env_int("LABS_CACHE_TTL_SEC", 30)

app = Flask(__name__)
app.secret_key = FLASK_SECRET

# Hardening cookies (no rompe nada si estás detrás de HTTPS)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE=os.getenv("SESSION_COOKIE_SAMESITE", "Lax"),
)

jobs = {}

# ---------------- Cache TTL simple ----------------

_cache = {}
_cache_lock = threading.Lock()

def _cache_get(key: str):
    with _cache_lock:
        v = _cache.get(key)
        if not v:
            return None
        exp_ts, data = v
        if time.time() >= exp_ts:
            _cache.pop(key, None)
            return None
        return data

def _cache_set(key: str, ttl_sec: int, data):
    with _cache_lock:
        _cache[key] = (time.time() + ttl_sec, data)

def _cache_clear(prefix: str = None):
    with _cache_lock:
        if prefix is None:
            _cache.clear()
            return
        for k in list(_cache.keys()):
            if k.startswith(prefix):
                _cache.pop(k, None)

# ---------------- Usuarios (SQLite) ----------------

def db_connect():
    conn = sqlite3.connect(USER_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_user_db():
    """
    Crea las tablas de usuarios y laboratorios si no existen y asegura
    que exista el usuario admin inicial.
    """
    conn = db_connect()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin','profesor')),
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()

        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_labs (
                username TEXT NOT NULL,
                lab_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (username, lab_name),
                FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE
            )
        """)
        conn.commit()

        cur = conn.execute("SELECT id FROM users WHERE username = ?", (AUTH_USER,))
        row = cur.fetchone()
        if not row:
            conn.execute(
                "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
                (AUTH_USER, generate_password_hash(AUTH_PASS), "admin", datetime.now().isoformat(timespec="seconds"))
            )
            conn.commit()
    finally:
        conn.close()

def get_user(username: str):
    conn = db_connect()
    try:
        cur = conn.execute("SELECT username, password_hash, role FROM users WHERE username = ?", (username,))
        return cur.fetchone()
    finally:
        conn.close()

def list_users():
    conn = db_connect()
    try:
        cur = conn.execute("SELECT username, role, created_at FROM users ORDER BY username COLLATE NOCASE")
        return cur.fetchall()
    finally:
        conn.close()

def create_user(username: str, password: str, role: str):
    conn = db_connect()
    try:
        conn.execute(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
            (username, generate_password_hash(password), role, datetime.now().isoformat(timespec="seconds"))
        )
        conn.commit()
    finally:
        conn.close()

def set_user_role(username: str, role: str):
    conn = db_connect()
    try:
        conn.execute("UPDATE users SET role = ? WHERE username = ?", (role, username))
        conn.commit()
    finally:
        conn.close()

def set_user_password(username: str, new_password: str):
    conn = db_connect()
    try:
        conn.execute("UPDATE users SET password_hash = ? WHERE username = ?",
                     (generate_password_hash(new_password), username))
        conn.commit()
    finally:
        conn.close()

def delete_user(username: str):
    conn = db_connect()
    try:
        conn.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()
    finally:
        conn.close()

def list_user_labs(username: str):
    conn = db_connect()
    try:
        rows = conn.execute(
            "SELECT lab_name FROM user_labs WHERE username = ? ORDER BY lab_name",
            (username,)
        ).fetchall()
        return [r["lab_name"] for r in rows]
    finally:
        conn.close()

def user_labs_map():
    """dict username -> [lab_name, ...]"""
    conn = db_connect()
    try:
        rows = conn.execute(
            "SELECT username, lab_name FROM user_labs ORDER BY username, lab_name"
        ).fetchall()
        m = {}
        for r in rows:
            m.setdefault(r["username"], []).append(r["lab_name"])
        return m
    finally:
        conn.close()

def set_user_labs(username: str, labs):
    labs_norm = sorted({(x or "").strip() for x in labs if (x or "").strip()})
    now = datetime.now().isoformat(sep=" ", timespec="seconds")

    conn = db_connect()
    try:
        conn.execute("DELETE FROM user_labs WHERE username = ?", (username,))
        for lab in labs_norm:
            conn.execute(
                "INSERT OR IGNORE INTO user_labs (username, lab_name, created_at) VALUES (?, ?, ?)",
                (username, lab, now)
            )
        conn.commit()
    finally:
        conn.close()

def clear_user_labs(username: str):
    conn = db_connect()
    try:
        conn.execute("DELETE FROM user_labs WHERE username = ?", (username,))
        conn.commit()
    finally:
        conn.close()

# ---------------- Auth / roles ----------------

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("authed"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return wrapper

def role_required(*roles):
    def deco(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not session.get("authed"):
                return redirect(url_for("login", next=request.path))
            role = session.get("role")
            if role not in roles:
                flash("No tienes permisos para acceder a esta sección.", "warning")
                return redirect(url_for("home"))
            return f(*args, **kwargs)
        return wrapper
    return deco


# ---------------- vCenter selector (admin) ----------------

def _vcenter_choices():
    """Lista de vCenters para UI: [{id,label,host}, ...]"""
    return [{"id": cfg["id"], "label": cfg["label"], "host": cfg["host"]} for cfg in VCENTERS.values()]


@app.context_processor
def inject_vcenter_context():
    vcid = get_active_vcenter_id()
    active = VCENTERS.get(vcid) if vcid else None
    return {
        "vcenters_nav": _vcenter_choices(),
        "active_vcenter_id": vcid,
        "active_vcenter_label": (active["label"] if active else None),
        "active_vcenter_host": (active["host"] if active else None),
    }


@app.before_request
def _require_admin_vcenter_selected():
    """Admin debe seleccionar vCenter antes de operar (evita acciones por defecto en el equivocado)."""
    if not has_request_context():
        return
    if not session.get("authed"):
        return
    if session.get("role") != "admin":
        return
    if session.get("vcenter_id"):
        return

    ep = request.endpoint or ""
    allowed = {"home", "login", "logout", "select_vcenter", "account_password", "static"}
    if ep in allowed or ep.startswith("static"):
        return

    flash("Selecciona el vCenter con el que quieres trabajar.", "warning")
    return redirect(url_for("home"))


@app.post("/vcenter/select")
@login_required
@role_required("admin")
def select_vcenter():
    vcid = (request.form.get("vcenter_id") or "").strip()
    if vcid not in VCENTERS:
        flash("vCenter inválido.", "danger")
        return redirect(url_for("home"))

    session["vcenter_id"] = vcid
    flash(f"vCenter activo: <strong>{VCENTERS[vcid]['label']}</strong> ({VCENTERS[vcid]['host']})", "success")

    nxt = request.form.get("next") or url_for("home")
    return redirect(nxt)


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = (request.form.get("username") or "").strip()
        p = request.form.get("password") or ""

        user = get_user(u)
        if user and check_password_hash(user["password_hash"], p):
            session["authed"] = True
            session["username"] = user["username"]
            session["role"] = user["role"]

            # vCenter activo:
            #  - admin debe elegirlo tras loguear (no se asume por defecto)
            #  - profesor queda fijado a PROF_VCENTER
            if user["role"] == "admin":
                session.pop("vcenter_id", None)
            else:
                session["vcenter_id"] = PROF_VCENTER

            nxt = request.args.get("next") or url_for("home")
            return redirect(nxt)

        flash("Credenciales inválidas", "danger")

    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ---------------- vCenter helpers ----------------

def connect_vcenter(vcenter_id=None):
    vcid, cfg = get_vcenter_cfg(vcenter_id)
    host = cfg["host"]
    user = cfg["user"]
    pwd = cfg["password"]
    verify_ssl = bool(cfg.get("verify_ssl"))

    if verify_ssl:
        si = connect.SmartConnect(host=host, user=user, pwd=pwd)
    else:
        context = ssl._create_unverified_context()
        si = connect.SmartConnect(host=host, user=user, pwd=pwd, sslContext=context)
    return si

def disconnect_vcenter(si):
    try:
        connect.Disconnect(si)
    except Exception:
        pass

@contextmanager
def vcenter_ctx(vcenter_id=None):
    """Context manager para asegurar Disconnect. Acepta vcenter_id para threads/acciones internas."""
    si = None
    try:
        si = connect_vcenter(vcenter_id)
        content = si.RetrieveContent()
        yield si, content
    finally:
        if si:
            disconnect_vcenter(si)

def get_obj_by_name(content, vimtype_list, name):
    view = content.viewManager.CreateContainerView(content.rootFolder, vimtype_list, True)
    try:
        for obj in view.view:
            if obj.name == name:
                return obj
    finally:
        view.Destroy()
    return None

def list_lab_folders(content, prefix=LAB_FOLDER_PREFIX):
    labs = []
    for dc in content.rootFolder.childEntity:
        try:
            rootf = dc.vmFolder
            for f in rootf.childEntity:
                if isinstance(f, vim.Folder) and f.name.startswith(prefix):
                    labs.append(f.name)
                if isinstance(f, vim.Folder):
                    for sub in f.childEntity:
                        if isinstance(sub, vim.Folder) and sub.name.startswith(prefix):
                            labs.append(sub.name)
        except Exception:
            continue
    return sorted(set(labs), key=lambda x: x.lower())

def find_folder_by_name(content, folder_name):
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.Folder], True)
    try:
        for f in view.view:
            if f.name == folder_name:
                return f
    finally:
        view.Destroy()
    return None

def _collect_snapshots_from_tree(tree_list):
    out = []
    if not tree_list:
        return out
    for node in tree_list:
        out.append(node)
        try:
            if node.childSnapshotList:
                out.extend(_collect_snapshots_from_tree(node.childSnapshotList))
        except Exception:
            pass
    return out

def get_snapshot_info(vm):
    """
    Retorna:
      has_snapshot (bool),
      snapshot_count (int),
      latest_name (str|None),
      latest_obj (vim.vm.Snapshot|None)
    """
    try:
        snap = getattr(vm, "snapshot", None)
        if not snap or not getattr(snap, "rootSnapshotList", None):
            return False, 0, None, None

        nodes = _collect_snapshots_from_tree(snap.rootSnapshotList)
        if not nodes:
            return False, 0, None, None

        def _ctime(n):
            try:
                return n.createTime
            except Exception:
                return datetime.fromtimestamp(0)

        latest_node = max(nodes, key=_ctime)
        latest_name = getattr(latest_node, "name", None)
        latest_obj = getattr(latest_node, "snapshot", None)

        return True, len(nodes), latest_name, latest_obj
    except Exception:
        return False, 0, None, None

def list_vms_in_folder(content, folder_obj):
    view = content.viewManager.CreateContainerView(folder_obj, [vim.VirtualMachine], True)
    vms = []
    try:
        for vm in view.view:
            try:
                if vm.config and vm.config.template:
                    continue
            except Exception:
                pass

            power = "unknown"
            try:
                power = str(vm.runtime.powerState)
            except Exception:
                power = "unknown"

            has_snapshot, snapshot_count, latest_name, _ = get_snapshot_info(vm)

            vms.append({
                "name": vm.name,
                "moid": vm._moId,
                "power": power,
                "has_snapshot": has_snapshot,
                "snapshot_count": snapshot_count,
                "latest_snapshot_name": latest_name
            })
    finally:
        view.Destroy()
    return sorted(vms, key=lambda x: x["name"].lower())

def count_vms_in_folder(content, folder_obj) -> int:
    """Cuenta VMs en una carpeta (excluye templates)."""
    view = content.viewManager.CreateContainerView(folder_obj, [vim.VirtualMachine], True)
    try:
        n = 0
        for vm in view.view:
            try:
                if vm.config and vm.config.template:
                    continue
            except Exception:
                pass
            n += 1
        return n
    finally:
        view.Destroy()

def find_vm_by_moid(content, moid: str):
    view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
    try:
        for vm in view.view:
            if getattr(vm, "_moId", None) == moid:
                return vm
    finally:
        view.Destroy()
    return None

def wait_for_power_state(vm, desired_state: str, timeout_sec: int) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            if str(vm.runtime.powerState) == desired_state:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False

# ---------------- ACL helpers (Labs / VMs) ----------------

def can_access_lab(lab_name: str) -> bool:
    if session.get("role") == "admin":
        return True
    username = session.get("username") or ""
    allowed = set(list_user_labs(username))
    return lab_name in allowed

def ensure_lab_access(lab_name: str) -> bool:
    if not can_access_lab(lab_name):
        flash("No tienes permisos para administrar este laboratorio.", "danger")
        return False
    return True

def vm_is_in_lab(vm, lab_name: str) -> bool:
    """True si la VM está dentro de la carpeta lab_name (o subcarpetas)."""
    try:
        cur = getattr(vm, "parent", None)
        while cur is not None:
            if isinstance(cur, vim.Folder) and cur.name == lab_name:
                return True
            cur = getattr(cur, "parent", None)
    except Exception:
        pass
    return False

def can_access_vm_without_lab(vm) -> bool:
    """
    Para endpoints que no reciben lab_name (ej: /api/vm/power),
    valida que la VM pertenezca a ALGUNO de los labs asignados al profesor.
    """
    if session.get("role") == "admin":
        return True

    username = session.get("username") or ""
    allowed = set(list_user_labs(username))
    if not allowed:
        return False

    try:
        cur = getattr(vm, "parent", None)
        while cur is not None:
            if isinstance(cur, vim.Folder) and cur.name in allowed:
                return True
            cur = getattr(cur, "parent", None)
    except Exception:
        pass

    return False

# ---------------- Job runner ----------------

def job_logger_put(job_id, msg):
    q = jobs[job_id]["queue"]
    ts = datetime.now().strftime("%H:%M:%S")
    q.put(f"[{ts}] {msg}")

def build_network_device_change(vm_template, target_network):
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

def _inventory_loader(vcenter_id=None):
    with vcenter_ctx(vcenter_id) as (_, content):
        templates = []
        vms_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        try:
            for vm in vms_view.view:
                try:
                    if vm.config and vm.config.template:
                        templates.append(vm.name)
                except Exception:
                    continue
        finally:
            vms_view.Destroy()

        folders = []
        for dc in content.rootFolder.childEntity:
            try:
                rootf = dc.vmFolder
                for f in rootf.childEntity:
                    if isinstance(f, vim.Folder):
                        folders.append(f.name)
                        for sub in f.childEntity:
                            if isinstance(sub, vim.Folder):
                                folders.append(sub.name)
            except Exception:
                continue

        hosts = []
        host_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
        try:
            for h in host_view.view:
                hosts.append(h.name)
        finally:
            host_view.Destroy()

        datastores = []
        ds_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.Datastore], True)
        try:
            for d in ds_view.view:
                datastores.append(d.name)
        finally:
            ds_view.Destroy()

        networks = []
        net_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.Network], True)
        try:
            for n in net_view.view:
                if n.name not in networks:
                    networks.append(n.name)
        finally:
            net_view.Destroy()

        dvs_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.dvs.DistributedVirtualPortgroup], True)
        try:
            for pg in dvs_view.view:
                if pg.name not in networks:
                    networks.append(pg.name)
        finally:
            dvs_view.Destroy()

        return (
            sorted(templates),
            sorted(set(folders)),
            sorted(hosts),
            sorted(datastores),
            sorted(networks),
        )

def list_templates_folders_hosts_datastores_networks(vcenter_id=None):
    vcid, _cfg = get_vcenter_cfg(vcenter_id)
    cache_key = f"inventory:{vcid}:v1"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    data = _inventory_loader(vcid)
    _cache_set(cache_key, INVENTORY_CACHE_TTL_SEC, data)
    return data

def _read_csv_users(csv_path: str):
    usuarios = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            u = (row[0] or "").strip()
            if not u or u.lower() == "usuario":
                continue
            usuarios.append(u)
    return usuarios

def _existing_vm_names_in_folder(content, folder_obj):
    names = set()
    view = content.viewManager.CreateContainerView(folder_obj, [vim.VirtualMachine], True)
    try:
        for vm in view.view:
            try:
                if vm.config and vm.config.template:
                    continue
            except Exception:
                pass
            try:
                names.add(vm.name)
            except Exception:
                pass
    finally:
        view.Destroy()
    return names

def clone_and_assign(vcenter_id, job_id, template_name, folder_name, csv_path, datastore_name, network_name,
                     host_sequence, power_on, make_snapshot, make_teacher, teacher_user, teacher_host):
    try:
        job_logger_put(job_id, f"[START] Job {job_id}: Template='{template_name}', Folder='{folder_name}', "
                               f"DS='{datastore_name}', Net='{network_name}', PowerOn={power_on}, Snapshot={make_snapshot}")

        with vcenter_ctx(vcenter_id) as (_, content):
            template = get_obj_by_name(content, [vim.VirtualMachine], template_name)
            if not template or not getattr(getattr(template, "config", None), "template", False):
                job_logger_put(job_id, f"[ERROR] Template '{template_name}' no encontrada o no es template.")
                jobs[job_id]["status"] = "error"
                return

            dest_folder = get_obj_by_name(content, [vim.Folder], folder_name)
            if not dest_folder:
                job_logger_put(job_id, f"[INFO] Carpeta '{folder_name}' no existe. Creándola...")
                datacenter = content.rootFolder.childEntity[0]
                dest_folder = datacenter.vmFolder.CreateFolder(folder_name)
                time.sleep(1)
                job_logger_put(job_id, f"[OK] Carpeta '{folder_name}' creada.")
                _cache_clear("labs:")  # el listado de labs puede cambiar

            datastore = get_obj_by_name(content, [vim.Datastore], datastore_name)
            if not datastore:
                job_logger_put(job_id, f"[ERROR] Datastore '{datastore_name}' no encontrado.")
                jobs[job_id]["status"] = "error"
                return

            network = get_obj_by_name(content, [vim.dvs.DistributedVirtualPortgroup], network_name)
            if not network:
                network = get_obj_by_name(content, [vim.Network], network_name)
            if not network:
                job_logger_put(job_id, f"[ERROR] Red '{network_name}' no encontrada.")
                jobs[job_id]["status"] = "error"
                return

            nic_changes = build_network_device_change(template, network)

            usuarios = _read_csv_users(csv_path)
            if not usuarios:
                job_logger_put(job_id, "[ERROR] El CSV no contiene usuarios válidos.")
                jobs[job_id]["status"] = "error"
                return

            if len(host_sequence) != len(usuarios):
                job_logger_put(job_id, f"[ERROR] La secuencia de hosts ({len(host_sequence)}) no coincide "
                                       f"con usuarios ({len(usuarios)}).")
                jobs[job_id]["status"] = "error"
                return

            host_cache = {}
            rp_cache = {}

            # Evita búsquedas repetidas y conflictos por nombre en la carpeta destino
            existing_names = _existing_vm_names_in_folder(content, dest_folder)

            for i, username in enumerate(usuarios, start=1):
                host_name = host_sequence[i - 1]

                if host_name not in host_cache:
                    h = get_obj_by_name(content, [vim.HostSystem], host_name)
                    if not h:
                        job_logger_put(job_id, f"[{i}] [ERROR] Host '{host_name}' no encontrado, saltando {username}.")
                        continue
                    host_cache[host_name] = h
                    rp_cache[host_name] = h.parent.resourcePool if hasattr(h.parent, "resourcePool") else None

                host = host_cache[host_name]
                resource_pool = rp_cache[host_name]

                vm_name = f"{folder_name}-{username}"

                if vm_name in existing_names:
                    job_logger_put(job_id, f"[{i}] SKIP: Ya existe '{vm_name}' en la carpeta; no se clona.")
                    continue

                job_logger_put(job_id, f"[{i}] Clonando '{vm_name}' en host '{host_name}'...")

                relocate_spec = vim.vm.RelocateSpec()
                relocate_spec.host = host
                if resource_pool:
                    relocate_spec.pool = resource_pool
                relocate_spec.datastore = datastore

                config_spec = vim.vm.ConfigSpec()
                if nic_changes:
                    config_spec.deviceChange = nic_changes

                clone_spec = vim.vm.CloneSpec(location=relocate_spec, powerOn=power_on, template=False, config=config_spec)

                try:
                    task = template.Clone(folder=dest_folder, name=vm_name, spec=clone_spec)
                except Exception as e:
                    job_logger_put(job_id, f"[{i}] [ERROR] No se pudo iniciar el clon de '{vm_name}': {e}")
                    continue

                vm_obj = None
                while True:
                    info = task.info
                    if info.state == vim.TaskInfo.State.success:
                        vm_obj = getattr(info, "result", None)
                        job_logger_put(job_id, f"[{i}] Clone OK: {vm_name}")
                        existing_names.add(vm_name)
                        break
                    elif info.state == vim.TaskInfo.State.error:
                        err_msg = getattr(info.error, "msg", str(info.error)) if info.error else "desconocido"
                        job_logger_put(job_id, f"[{i}] Clone ERROR: {vm_name}  Error: {err_msg}")
                        break
                    time.sleep(1)

                if not vm_obj:
                    # fallback
                    vm_obj = get_obj_by_name(content, [vim.VirtualMachine], vm_name)

                if not vm_obj:
                    job_logger_put(job_id, f"[{i}] [WARN] VM {vm_name} no encontrada tras clonar; permisos omitidos.")
                    continue


                if make_snapshot:
                    try:
                        snap_name = f"baseline-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
                        job_logger_put(job_id, f"[{i}] Snapshot creando: {vm_name} → {snap_name}")
                        snap_task = vm_obj.CreateSnapshot_Task(
                            name=snap_name,
                            description="Instantánea inicial creada por VM Labs Portal",
                            memory=False,
                            quiesce=False
                        )
                        while True:
                            sinfo = snap_task.info
                            if sinfo.state == vim.TaskInfo.State.success:
                                job_logger_put(job_id, f"[{i}] Snapshot OK: {vm_name} → {snap_name}")
                                break
                            elif sinfo.state == vim.TaskInfo.State.error:
                                s_err = getattr(sinfo.error, "msg", str(sinfo.error)) if sinfo.error else "desconocido"
                                job_logger_put(job_id, f"[{i}] Snapshot ERROR: {vm_name} → {snap_name}  Error: {s_err}")
                                break
                            time.sleep(1)
                    except Exception as e:
                        job_logger_put(job_id, f"[{i}] [ERROR] Creando snapshot en {vm_name}: {e}")

            # VM del profesor (opcional)
            if make_teacher and teacher_user:
                try:
                    teacher_vm_name = f"{folder_name}-{teacher_user}"

                    datacenter = content.rootFolder.childEntity[0]
                    prof_folder = get_obj_by_name(content, [vim.Folder], "Profesores")
                    if not prof_folder:
                        job_logger_put(job_id, "[PROF] Carpeta 'Profesores' no existe. Creándola...")
                        prof_folder = datacenter.vmFolder.CreateFolder("Profesores")
                        time.sleep(1)

                    host_for_teacher = None
                    if teacher_host:
                        host_for_teacher = get_obj_by_name(content, [vim.HostSystem], teacher_host)

                    if not host_for_teacher:
                        first_host_name = host_sequence[0] if host_sequence else None
                        host_for_teacher = get_obj_by_name(content, [vim.HostSystem], first_host_name) if first_host_name else None

                    if not host_for_teacher:
                        job_logger_put(job_id, "[PROF] [ERROR] No se pudo resolver host para la VM del profesor; se omite.")
                    else:
                        rp_for_teacher = host_for_teacher.parent.resourcePool if hasattr(host_for_teacher.parent, "resourcePool") else None

                        # Para evitar duplicados dentro de Profesores
                        existing_prof_names = _existing_vm_names_in_folder(content, prof_folder)
                        if teacher_vm_name in existing_prof_names:
                            job_logger_put(job_id, f"[PROF] SKIP: Ya existe '{teacher_vm_name}' en carpeta Profesores.")
                        else:
                            job_logger_put(job_id, f"[PROF] Clonando '{teacher_vm_name}' en host '{host_for_teacher.name}'...")

                            relocate_spec_p = vim.vm.RelocateSpec()
                            relocate_spec_p.host = host_for_teacher
                            if rp_for_teacher:
                                relocate_spec_p.pool = rp_for_teacher
                            relocate_spec_p.datastore = datastore

                            config_spec_p = vim.vm.ConfigSpec()
                            if nic_changes:
                                config_spec_p.deviceChange = nic_changes

                            clone_spec_p = vim.vm.CloneSpec(
                                location=relocate_spec_p,
                                powerOn=power_on,
                                template=False,
                                config=config_spec_p
                            )

                            task_p = template.Clone(folder=prof_folder, name=teacher_vm_name, spec=clone_spec_p)

                            vm_prof = None
                            while True:
                                info_p = task_p.info
                                if info_p.state == vim.TaskInfo.State.success:
                                    vm_prof = getattr(info_p, "result", None)
                                    job_logger_put(job_id, f"[PROF] Clone OK: {teacher_vm_name}")
                                    break
                                elif info_p.state == vim.TaskInfo.State.error:
                                    errp = getattr(info_p.error, "msg", str(info_p.error)) if info_p.error else "desconocido"
                                    job_logger_put(job_id, f"[PROF] Clone ERROR: {teacher_vm_name}  Error: {errp}")
                                    break
                                time.sleep(1)

                            if not vm_prof:
                                vm_prof = get_obj_by_name(content, [vim.VirtualMachine], teacher_vm_name)
                                if make_snapshot:
                                    try:
                                        snap_name = f"baseline-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
                                        job_logger_put(job_id, f"[PROF] Snapshot creando: {teacher_vm_name} → {snap_name}")
                                        snap_task = vm_prof.CreateSnapshot_Task(
                                            name=snap_name,
                                            description="Instantánea inicial (profesor) creada por VM Labs Portal",
                                            memory=False,
                                            quiesce=False
                                        )
                                        while True:
                                            sinfo = snap_task.info
                                            if sinfo.state == vim.TaskInfo.State.success:
                                                job_logger_put(job_id, f"[PROF] Snapshot OK: {teacher_vm_name} → {snap_name}")
                                                break
                                            elif sinfo.state == vim.TaskInfo.State.error:
                                                s_err = getattr(sinfo.error, "msg", str(sinfo.error)) if sinfo.error else "desconocido"
                                                job_logger_put(job_id, f"[PROF] Snapshot ERROR: {teacher_vm_name} → {snap_name}  Error: {s_err}")
                                                break
                                            time.sleep(1)
                                    except Exception as e:
                                        job_logger_put(job_id, f"[PROF] [ERROR] Creando snapshot en {teacher_vm_name}: {e}")
                except Exception as e:
                    job_logger_put(job_id, f"[PROF] [ERROR] Excepción creando VM del profesor: {e}")

        job_logger_put(job_id, f"[END] Tarea {job_id} completada.")
        jobs[job_id]["status"] = "done"

    except Exception as e:
        job_logger_put(job_id, f"[FATAL] Excepción en la tarea {job_id}: {e}")
        jobs[job_id]["status"] = "error"

# ---------------- Vistas ----------------

@app.route("/")
@login_required
def home():
    return render_template("home.html")

@app.route("/create")
@login_required
@role_required("admin")
def create_lab():
    templates, folders, hosts, datastores, networks = list_templates_folders_hosts_datastores_networks()
    return render_template(
        "index.html",
        templates=templates,
        folders=folders,
        hosts=hosts,
        datastores=datastores,
        networks=networks
    )

@app.route("/labs")
@login_required
def labs():
    with vcenter_ctx() as (_, content):
        # Cachear solo el listado de labs (no conteos)
        vcid, _cfg = get_vcenter_cfg()
        cache_key = f"labs:{vcid}:list:{LAB_FOLDER_PREFIX}"
        lab_names = _cache_get(cache_key)
        if lab_names is None:
            lab_names = list_lab_folders(content, prefix=LAB_FOLDER_PREFIX)
            _cache_set(cache_key, LABS_CACHE_TTL_SEC, lab_names)

        empty_message = None
        if session.get("role") != "admin":
            allowed = set(list_user_labs(session.get("username") or ""))
            lab_names = [n for n in lab_names if n in allowed]
            empty_message = "No tienes laboratorios asignados. Pide al administrador que te los asigne."

        labs_data = []
        for name in lab_names:
            folder = find_folder_by_name(content, name)
            if not folder:
                labs_data.append({"name": name, "count": 0})
                continue
            try:
                c = count_vms_in_folder(content, folder)
            except Exception:
                c = 0
            labs_data.append({"name": name, "count": c})

        labs_data.sort(key=lambda x: x["name"].lower())
        return render_template("labs.html", labs=labs_data, prefix=LAB_FOLDER_PREFIX, empty_message=empty_message)

@app.route("/labs/<lab_name>")
@login_required
def lab_detail(lab_name):
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))

    with vcenter_ctx() as (_, content):
        folder = find_folder_by_name(content, lab_name)
        if not folder:
            flash("No se encontró la carpeta del lab en vCenter.", "danger")
            return redirect(url_for("labs"))

        vms = list_vms_in_folder(content, folder)

        return render_template(
            "lab_detail.html",
            lab_name=lab_name,
            vms=vms,
            vcenter_host=get_vcenter_cfg()[1]['host'],
            poll_interval_ms=POLL_INTERVAL_MS
        )

# -------- API power+snapshot --------
@app.get("/api/vm/power")
@login_required
def api_vm_power():
    moid = request.args.get("moid", "").strip()
    if not moid:
        return jsonify({"ok": False, "error": "missing moid"}), 400

    with vcenter_ctx() as (_, content):
        vm = find_vm_by_moid(content, moid)
        if not vm:
            return jsonify({"ok": False, "error": "vm not found"}), 404

        if session.get("role") != "admin":
            if not can_access_vm_without_lab(vm):
                return jsonify({"ok": False, "error": "forbidden"}), 403

        power = "unknown"
        try:
            power = str(vm.runtime.powerState)
        except Exception:
            power = "unknown"

        has_snapshot, snapshot_count, latest_name, _ = get_snapshot_info(vm)

        resp = jsonify({
            "ok": True,
            "moid": moid,
            "power": power,
            "has_snapshot": has_snapshot,
            "snapshot_count": snapshot_count,
            "latest_snapshot_name": latest_name
        })
        resp.headers["Cache-Control"] = "no-store"
        return resp

# -------- Snapshot create/delete --------
@app.post("/vm/snapshot/create")
@login_required
def vm_snapshot_create():
    moid = (request.form.get("moid") or "").strip()
    lab_name = (request.form.get("lab_name") or "").strip()

    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))
    if not moid:
        flash("Falta moid.", "danger")
        return redirect(url_for("lab_detail", lab_name=lab_name))

    with vcenter_ctx() as (_, content):
        vm = find_vm_by_moid(content, moid)
        if not vm:
            flash("VM no encontrada.", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        if session.get("role") != "admin" and not vm_is_in_lab(vm, lab_name):
            flash("No tienes permisos para operar esta VM.", "danger")
            return redirect(url_for("labs"))

        snap_name = f"manual-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        try:
            t = vm.CreateSnapshot_Task(
                name=snap_name,
                description="Snapshot manual creado desde VM Labs Portal",
                memory=False,
                quiesce=False
            )
            while True:
                info = t.info
                if info.state == vim.TaskInfo.State.success:
                    break
                if info.state == vim.TaskInfo.State.error:
                    err = getattr(info.error, "msg", str(info.error)) if info.error else "desconocido"
                    flash(f"Error creando snapshot: {err}", "danger")
                    return redirect(url_for("lab_detail", lab_name=lab_name))
                time.sleep(1)
        except Exception as e:
            flash(f"Error creando snapshot: {e}", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

    flash(f"Snapshot creado: '{snap_name}'. Usa 'Actualizar estados'.", "success")
    return redirect(url_for("lab_detail", lab_name=lab_name))

@app.post("/vm/snapshot/delete_last")
@login_required
def vm_snapshot_delete_last():
    moid = (request.form.get("moid") or "").strip()
    lab_name = (request.form.get("lab_name") or "").strip()
    confirm = (request.form.get("confirm") or "").strip().upper()

    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))

    if confirm != "SI":
        flash("Confirmación inválida para eliminar snapshot.", "danger")
        return redirect(url_for("lab_detail", lab_name=lab_name))

    if not moid:
        flash("Falta moid.", "danger")
        return redirect(url_for("lab_detail", lab_name=lab_name))

    with vcenter_ctx() as (_, content):
        vm = find_vm_by_moid(content, moid)
        if not vm:
            flash("VM no encontrada.", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        if session.get("role") != "admin" and not vm_is_in_lab(vm, lab_name):
            flash("No tienes permisos para operar esta VM.", "danger")
            return redirect(url_for("labs"))

        has_snapshot, snapshot_count, latest_name, latest_obj = get_snapshot_info(vm)
        if not has_snapshot or not latest_obj:
            flash("La VM no tiene snapshots.", "info")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        try:
            t = latest_obj.RemoveSnapshot_Task(removeChildren=False)
            while True:
                info = t.info
                if info.state == vim.TaskInfo.State.success:
                    break
                if info.state == vim.TaskInfo.State.error:
                    err = getattr(info.error, "msg", str(info.error)) if info.error else "desconocido"
                    flash(f"Error eliminando snapshot: {err}", "danger")
                    return redirect(url_for("lab_detail", lab_name=lab_name))
                time.sleep(1)
        except Exception as e:
            flash(f"Error eliminando snapshot: {e}", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

    flash(f"Snapshot eliminado: '{latest_name}'. Usa 'Actualizar estados'.", "warning")
    return redirect(url_for("lab_detail", lab_name=lab_name))

# -------- Power per VM --------
@app.post("/vm/power/on")
@login_required
def vm_power_on():
    moid = (request.form.get("moid") or "").strip()
    lab_name = (request.form.get("lab_name") or "").strip()

    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))
    if not moid:
        flash("Falta moid.", "danger")
        return redirect(url_for("labs"))

    with vcenter_ctx() as (_, content):
        vm = find_vm_by_moid(content, moid)
        if not vm:
            flash("VM no encontrada.", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        if session.get("role") != "admin" and not vm_is_in_lab(vm, lab_name):
            flash("No tienes permisos para operar esta VM.", "danger")
            return redirect(url_for("labs"))

        if str(vm.runtime.powerState) == "poweredOn":
            flash("La VM ya está encendida.", "info")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        vm.PowerOnVM_Task()

    flash("Encendido solicitado. Usa 'Actualizar estados'.", "success")
    return redirect(url_for("lab_detail", lab_name=lab_name))

@app.post("/vm/power/off")
@login_required
def vm_power_off():
    moid = (request.form.get("moid") or "").strip()
    lab_name = (request.form.get("lab_name") or "").strip()

    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))
    if not moid:
        flash("Falta moid.", "danger")
        return redirect(url_for("labs"))

    with vcenter_ctx() as (_, content):
        vm = find_vm_by_moid(content, moid)
        if not vm:
            flash("VM no encontrada.", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        if session.get("role") != "admin" and not vm_is_in_lab(vm, lab_name):
            flash("No tienes permisos para operar esta VM.", "danger")
            return redirect(url_for("labs"))

        if str(vm.runtime.powerState) == "poweredOff":
            flash("La VM ya está apagada.", "info")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        tools_status = "unknown"
        try:
            tools_status = str(vm.guest.toolsRunningStatus)
        except Exception:
            tools_status = "unknown"

        try:
            vm.ShutdownGuest()
            ok = wait_for_power_state(vm, "poweredOff", SOFT_SHUTDOWN_TIMEOUT_SEC)
            if ok:
                flash("Apagado amable completado (ShutdownGuest).", "success")
                return redirect(url_for("lab_detail", lab_name=lab_name))

            if SOFT_SHUTDOWN_FALLBACK_HARD:
                vm.PowerOffVM_Task()
                flash(f"No se apagó en {SOFT_SHUTDOWN_TIMEOUT_SEC}s. Se aplicó PowerOff (hard).", "warning")
            else:
                flash(f"ShutdownGuest enviado, pero no apagó en {SOFT_SHUTDOWN_TIMEOUT_SEC}s. Tools={tools_status}.", "warning")

            return redirect(url_for("lab_detail", lab_name=lab_name))

        except Exception as e:
            if SOFT_SHUTDOWN_FALLBACK_HARD:
                try:
                    vm.PowerOffVM_Task()
                    flash(f"ShutdownGuest falló ({e}). Se aplicó PowerOff (hard).", "warning")
                except Exception as e2:
                    flash(f"ShutdownGuest falló ({e}) y PowerOff falló ({e2}).", "danger")
            else:
                flash(f"Error apagando (ShutdownGuest): {e}. Tools={tools_status}.", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

@app.post("/vm/power/reboot")
@login_required
def vm_power_reboot():
    moid = (request.form.get("moid") or "").strip()
    lab_name = (request.form.get("lab_name") or "").strip()

    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))
    if not moid:
        flash("Falta moid.", "danger")
        return redirect(url_for("labs"))

    with vcenter_ctx() as (_, content):
        vm = find_vm_by_moid(content, moid)
        if not vm:
            flash("VM no encontrada.", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        if session.get("role") != "admin" and not vm_is_in_lab(vm, lab_name):
            flash("No tienes permisos para operar esta VM.", "danger")
            return redirect(url_for("labs"))

        if str(vm.runtime.powerState) != "poweredOn":
            flash("Para reiniciar, la VM debe estar encendida.", "warning")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        try:
            vm.RebootGuest()
            flash("Reinicio amable solicitado (RebootGuest).", "success")
        except Exception as e:
            if SOFT_REBOOT_FALLBACK_RESET:
                try:
                    vm.ResetVM_Task()
                    flash(f"RebootGuest falló ({e}). Se aplicó Reset (hard).", "warning")
                except Exception as e2:
                    flash(f"RebootGuest falló ({e}) y Reset falló ({e2}).", "danger")
            else:
                flash(f"Error reiniciando (RebootGuest): {e}", "danger")

    return redirect(url_for("lab_detail", lab_name=lab_name))

# -------- Power por LAB completo --------
@app.post("/lab/power/on")
@login_required
def lab_power_on():
    lab_name = (request.form.get("lab_name") or "").strip()
    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))

    with vcenter_ctx() as (_, content):
        folder = find_folder_by_name(content, lab_name)
        if not folder:
            flash("Carpeta de lab no encontrada.", "danger")
            return redirect(url_for("labs"))

        view = content.viewManager.CreateContainerView(folder, [vim.VirtualMachine], True)
        to_power = []
        try:
            for vm in view.view:
                try:
                    if vm.config and vm.config.template:
                        continue
                except Exception:
                    pass
                try:
                    if str(vm.runtime.powerState) != "poweredOn":
                        to_power.append(vm)
                except Exception:
                    to_power.append(vm)
        finally:
            view.Destroy()

        started = 0
        for vm in to_power:
            try:
                vm.PowerOnVM_Task()
                started += 1
            except Exception:
                pass

    flash(f"Encendido solicitado para {started} VM(s). Usa 'Actualizar estados'.", "success")
    return redirect(url_for("lab_detail", lab_name=lab_name))

@app.post("/lab/power/off")
@login_required
def lab_power_off():
    lab_name = (request.form.get("lab_name") or "").strip()
    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))

    with vcenter_ctx() as (_, content):
        folder = find_folder_by_name(content, lab_name)
        if not folder:
            flash("Carpeta de lab no encontrada.", "danger")
            return redirect(url_for("labs"))

        view = content.viewManager.CreateContainerView(folder, [vim.VirtualMachine], True)
        vms = []
        try:
            for vm in view.view:
                try:
                    if vm.config and vm.config.template:
                        continue
                except Exception:
                    pass
                vms.append(vm)
        finally:
            view.Destroy()

        requested = 0
        hard_fallback = 0

        for vm in vms:
            try:
                if str(vm.runtime.powerState) == "poweredOff":
                    continue
            except Exception:
                pass

            try:
                vm.ShutdownGuest()
                requested += 1
            except Exception:
                if SOFT_SHUTDOWN_FALLBACK_HARD:
                    try:
                        vm.PowerOffVM_Task()
                        hard_fallback += 1
                    except Exception:
                        pass

    msg = f"Apagado amable solicitado para {requested} VM(s)."
    if SOFT_SHUTDOWN_FALLBACK_HARD and hard_fallback:
        msg += f" Fallback PowerOff aplicado a {hard_fallback} VM(s)."
    flash(msg + " Usa 'Actualizar estados'.", "warning")
    return redirect(url_for("lab_detail", lab_name=lab_name))

# -------- Recreate VM (revert to snapshot) --------
@app.post("/vm/recreate")
@login_required
def vm_recreate():
    moid = (request.form.get("moid") or "").strip()
    lab_name = (request.form.get("lab_name") or "").strip()
    confirm = (request.form.get("confirm") or "").strip().upper()

    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))

    if confirm != "SI":
        flash("Confirmación inválida para recrear VM.", "danger")
        return redirect(url_for("lab_detail", lab_name=lab_name))

    with vcenter_ctx() as (_, content):
        vm = find_vm_by_moid(content, moid)
        if not vm:
            flash("VM no encontrada.", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        if session.get("role") != "admin" and not vm_is_in_lab(vm, lab_name):
            flash("No tienes permisos para operar esta VM.", "danger")
            return redirect(url_for("labs"))

        if getattr(vm, "snapshot", None) is None or not getattr(vm.snapshot, "rootSnapshotList", None):
            flash("La VM no tiene snapshots. No se puede recrear.", "warning")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        try:
            root = vm.snapshot.rootSnapshotList[0]
        except Exception:
            flash("No se pudo determinar snapshot baseline.", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        snap_name = getattr(root, "name", "baseline")
        snap_obj = root.snapshot

        t = snap_obj.RevertToSnapshot_Task()
        while True:
            info = t.info
            if info.state == vim.TaskInfo.State.success:
                break
            if info.state == vim.TaskInfo.State.error:
                err = getattr(info.error, "msg", str(info.error)) if info.error else "desconocido"
                flash(f"Error recreando VM (revert snapshot): {err}", "danger")
                return redirect(url_for("lab_detail", lab_name=lab_name))
            time.sleep(1)

    flash(f"VM restablecida al snapshot '{snap_name}'. (Queda apagada). Usa 'Actualizar estados'.", "success")
    return redirect(url_for("lab_detail", lab_name=lab_name))

# -------- Delete Lab (ADMIN ONLY) --------
@app.post("/lab/delete")
@login_required
@role_required("admin")
def lab_delete():
    lab_name = (request.form.get("lab_name") or "").strip()
    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))

    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))

    with vcenter_ctx() as (_, content):
        folder = find_folder_by_name(content, lab_name)
        if not folder:
            flash("Carpeta de lab no encontrada.", "danger")
            return redirect(url_for("labs"))

        view = content.viewManager.CreateContainerView(folder, [vim.VirtualMachine], True)
        vms = []
        try:
            for vm in view.view:
                try:
                    if vm.config and vm.config.template:
                        continue
                except Exception:
                    pass
                vms.append(vm)
        finally:
            view.Destroy()

        # Best effort: elimina snapshots (sin bloquear demasiado)
        for vm in vms:
            try:
                if getattr(vm, "snapshot", None) is not None:
                    try:
                        vm.RemoveAllSnapshots_Task()
                    except Exception:
                        pass
            except Exception:
                pass

        deleted = 0
        for vm in vms:
            try:
                try:
                    if str(vm.runtime.powerState) == "poweredOn":
                        vm.PowerOffVM_Task()
                        time.sleep(1)
                except Exception:
                    pass

                vm.Destroy_Task()
                deleted += 1
            except Exception:
                pass

        try:
            folder.Destroy()
        except Exception:
            pass

    _cache_clear("labs:")
    flash(f"Lab eliminado. VMs borradas: {deleted}.", "success")
    return redirect(url_for("labs"))

# ---------------- Crear lab (con VM profesor) ----------------
@app.route("/plan_distribution", methods=["POST"])
@login_required
@role_required("admin")
def plan_distribution():
    template = request.form.get("template", "").strip()
    folder = request.form.get("folder", "").strip()
    new_folder = request.form.get("new_folder", "").strip()
    datastore = request.form.get("datastore", "").strip()
    network = request.form.get("network", "").strip()

    folder_name = new_folder if new_folder else folder
    if not all([template, folder_name, datastore, network]):
        flash("Faltan datos (plantilla, carpeta, datastore, red y CSV).", "danger")
        return redirect(url_for("create_lab"))

    make_teacher = request.form.get("make_teacher") == "on"
    teacher_user = request.form.get("teacher_user", "").strip()

    if "csvfile" not in request.files:
        flash("CSV requerido.", "danger")
        return redirect(url_for("create_lab"))
    f = request.files["csvfile"]
    if f.filename == "":
        flash("CSV requerido.", "danger")
        return redirect(url_for("create_lab"))

    pre_id = "plan_" + str(uuid.uuid4())[:8]
    csv_path = os.path.join(TMP_DIR, f"{pre_id}.csv")
    f.save(csv_path)

    usuarios = _read_csv_users(csv_path)
    total = len(usuarios)
    if total == 0:
        flash("El CSV no contiene usuarios válidos.", "warning")
        return redirect(url_for("create_lab"))

    _, _, hosts, _, _ = list_templates_folders_hosts_datastores_networks()
    if not hosts:
        flash("No hay hosts disponibles.", "danger")
        return redirect(url_for("create_lab"))

    base = total // len(hosts)
    resto = total % len(hosts)
    initial = {h: base for h in hosts}
    for i in range(resto):
        initial[hosts[i]] += 1

    return render_template("distribution.html",
                           total=total,
                           template=template,
                           folder=folder_name,
                           datastore=datastore,
                           network=network,
                           csv_path=csv_path,
                           hosts=hosts,
                           initial=initial,
                           make_teacher=make_teacher,
                           teacher_user=teacher_user)

@app.route("/start_job_with_distribution", methods=["POST"])
@login_required
@role_required("admin")
def start_job_with_distribution():
    template = request.form.get("template", "").strip()
    folder = request.form.get("folder", "").strip()
    datastore = request.form.get("datastore", "").strip()
    network = request.form.get("network", "").strip()
    csv_path = request.form.get("csv_path", "").strip()
    host_names = request.form.get("host_names", "").strip().split("|")
    power_on = request.form.get("power_on") == "on"
    make_snapshot = request.form.get("make_snapshot") == "on"

    make_teacher = request.form.get("make_teacher") == "on"
    teacher_user = request.form.get("teacher_user", "").strip()
    teacher_host = request.form.get("teacher_host", "").strip()

    if not all([template, folder, datastore, network, csv_path]) or not host_names:
        flash("Datos incompletos.", "danger")
        return redirect(url_for("create_lab"))

    counts = []
    total_counts = 0
    for idx, h in enumerate(host_names):
        val = request.form.get(f"host_{idx}", "0").strip()
        try:
            n = int(val)
            if n < 0:
                n = 0
        except Exception:
            n = 0
        counts.append((h, n))
        total_counts += n

    usuarios = _read_csv_users(csv_path)
    total_csv = len(usuarios)

    if total_counts != total_csv:
        flash(f"La suma de asignaciones ({total_counts}) no coincide con el total del CSV ({total_csv}).", "danger")
        return redirect(url_for("create_lab"))

    host_sequence = []
    for h, n in counts:
        host_sequence += [h] * n

    vcid, _cfg = get_vcenter_cfg()

    job_id = str(uuid.uuid4())[:8]
    q = Queue()
    jobs[job_id] = {"status": "running", "queue": q, "thread": None, "vcenter_id": vcid}

    t = threading.Thread(
        target=clone_and_assign,
        args=(vcid, job_id, template, folder, csv_path, datastore, network,
              host_sequence, power_on, make_snapshot, make_teacher, teacher_user, teacher_host),
        daemon=True
    )
    jobs[job_id]["thread"] = t
    t.start()

    return redirect(url_for("job_page", job_id=job_id))
@app.route("/job/<job_id>")
@login_required
def job_page(job_id):
    if job_id not in jobs:
        return render_template("job.html", job_id=job_id, missing=True)
    return render_template("job.html", job_id=job_id, missing=False)

@app.route("/stream/<job_id>")
@login_required
def stream(job_id):
    if job_id not in jobs:
        return "Job no encontrado", 404

    def event_stream(q):
        while True:
            try:
                line = q.get(timeout=1)
                yield f"data: {line}\n\n"
                if jobs[job_id]["status"] != "running" and q.empty():
                    break
            except Empty:
                if jobs[job_id]["status"] != "running" and q.empty():
                    break
                continue
        yield "data: [STREAM END]\n\n"

    return Response(event_stream(jobs[job_id]["queue"]), mimetype="text/event-stream")

@app.route("/api/templates")
@login_required
@role_required("admin")
def api_templates():
    t, *_ = list_templates_folders_hosts_datastores_networks()
    resp = jsonify(t)
    resp.headers["Cache-Control"] = "no-store"
    return resp

@app.route("/api/folders")
@login_required
@role_required("admin")
def api_folders():
    _, f, *_ = list_templates_folders_hosts_datastores_networks()
    resp = jsonify(f)
    resp.headers["Cache-Control"] = "no-store"
    return resp

# -------- Delete Single VM --------
@app.post("/vm/delete")
@login_required
def vm_delete():
    moid = (request.form.get("moid") or "").strip()
    lab_name = (request.form.get("lab_name") or "").strip()
    confirm = (request.form.get("confirm") or "").strip().upper()

    if not lab_name:
        flash("Falta lab_name.", "danger")
        return redirect(url_for("labs"))
    if not ensure_lab_access(lab_name):
        return redirect(url_for("labs"))

    if confirm != "SI":
        flash("Confirmación inválida para eliminar la VM.", "danger")
        return redirect(url_for("lab_detail", lab_name=lab_name))

    with vcenter_ctx() as (_, content):
        vm = find_vm_by_moid(content, moid)
        if not vm:
            flash("VM no encontrada.", "danger")
            return redirect(url_for("lab_detail", lab_name=lab_name))

        if session.get("role") != "admin" and not vm_is_in_lab(vm, lab_name):
            flash("No tienes permisos para operar esta VM.", "danger")
            return redirect(url_for("labs"))

        vm_name = vm.name

        try:
            if str(vm.runtime.powerState) != "poweredOff":
                vm.PowerOffVM_Task()
                time.sleep(2)
        except Exception:
            pass

        task_del = vm.Destroy_Task()
        while True:
            info = task_del.info
            if info.state == vim.TaskInfo.State.success:
                break
            if info.state == vim.TaskInfo.State.error:
                err = getattr(info.error, "msg", str(info.error))
                flash(f"Error al destruir VM: {err}", "danger")
                return redirect(url_for("lab_detail", lab_name=lab_name))
            time.sleep(1)

    flash(f"VM '{vm_name}' eliminada correctamente.", "warning")
    return redirect(url_for("lab_detail", lab_name=lab_name))

# ---------------- Admin Usuarios ----------------

@app.get("/users")
@login_required
@role_required("admin")
def users_admin():
    users = list_users()
    return render_template("admin_users.html", users=users, user_labs=user_labs_map())

@app.post("/users/create")
@login_required
@role_required("admin")
def users_create():
    username = (request.form.get("username") or "").strip()
    role = (request.form.get("role") or "").strip()
    pwd1 = request.form.get("password") or ""
    pwd2 = request.form.get("password2") or ""

    if not username:
        flash("Falta el usuario.", "danger")
        return redirect(url_for("users_admin"))

    if role not in ("admin", "profesor"):
        flash("Rol inválido.", "danger")
        return redirect(url_for("users_admin"))

    if len(pwd1) < 6:
        flash("La contraseña debe tener al menos 6 caracteres.", "warning")
        return redirect(url_for("users_admin"))

    if pwd1 != pwd2:
        flash("Las contraseñas no coinciden.", "warning")
        return redirect(url_for("users_admin"))

    if get_user(username):
        flash("Ese usuario ya existe.", "warning")
        return redirect(url_for("users_admin"))

    try:
        create_user(username, pwd1, role)
        flash("Usuario creado correctamente.", "success")
    except Exception as e:
        flash(f"Error creando usuario: {e}", "danger")

    return redirect(url_for("users_admin"))

@app.post("/users/<username>/role")
@login_required
@role_required("admin")
def users_set_role(username):
    role = (request.form.get("role") or "").strip()
    if role not in ("admin", "profesor"):
        flash("Rol inválido.", "danger")
        return redirect(url_for("users_admin"))

    if username == session.get("username"):
        flash("No puedes cambiar tu propio rol.", "warning")
        return redirect(url_for("users_admin"))

    set_user_role(username, role)

    if role != "profesor":
        clear_user_labs(username)

    flash("Rol actualizado.", "success")
    return redirect(url_for("users_admin"))

@app.route("/users/<username>/labs", methods=["GET", "POST"])
@login_required
@role_required("admin")
def users_set_labs(username):
    u = get_user(username)
    if not u:
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for("users_admin"))

    if request.method == "POST":
        labs = request.form.getlist("labs")
        if u["role"] != "profesor":
            flash("Solo se asignan laboratorios a usuarios con rol profesor.", "warning")
            clear_user_labs(username)
            return redirect(url_for("users_admin"))

        set_user_labs(username, labs)
        flash("Labs asignados.", "success")
        return redirect(url_for("users_admin"))

    assigned = set(list_user_labs(username))

    labs_data = []
    try:
        with vcenter_ctx(PROF_VCENTER) as (_, content):
            lab_names = list_lab_folders(content, prefix=LAB_FOLDER_PREFIX)
            for name in lab_names:
                folder = find_folder_by_name(content, name)
                try:
                    c = count_vms_in_folder(content, folder) if folder else 0
                except Exception:
                    c = 0
                labs_data.append({"name": name, "count": c})

            labs_data.sort(key=lambda x: x["name"].lower())
    except Exception as e:
        flash(f"No se pudo leer labs desde vCenter: {e}", "warning")

    return render_template("user_labs.html", user=u, labs=labs_data, assigned=assigned, prefix=LAB_FOLDER_PREFIX)

@app.route("/users/<username>/password", methods=["GET", "POST"])
@login_required
@role_required("admin")
def users_set_password(username):
    if request.method == "POST":
        pwd1 = request.form.get("password") or ""
        pwd2 = request.form.get("password2") or ""
        if len(pwd1) < 6:
            flash("La contraseña debe tener al menos 6 caracteres.", "warning")
            return redirect(url_for("users_set_password", username=username))
        if pwd1 != pwd2:
            flash("Las contraseñas no coinciden.", "warning")
            return redirect(url_for("users_set_password", username=username))

        set_user_password(username, pwd1)
        flash("Contraseña actualizada.", "success")
        return redirect(url_for("users_admin"))

    return render_template("user_password.html", target_user=username)

@app.post("/users/<username>/delete")
@login_required
@role_required("admin")
def users_delete(username):
    if username == session.get("username"):
        flash("No puedes borrarte a ti mismo.", "warning")
        return redirect(url_for("users_admin"))

    if not get_user(username):
        flash("El usuario no existe.", "warning")
        return redirect(url_for("users_admin"))

    delete_user(username)
    flash("Usuario eliminado.", "success")
    return redirect(url_for("users_admin"))

# ---------------- Mi Cuenta: Cambiar contraseña ----------------

@app.route("/account/password", methods=["GET", "POST"])
@login_required
def account_password():
    me = session.get("username")
    user = get_user(me)

    if request.method == "POST":
        current = request.form.get("current") or ""
        pwd1 = request.form.get("password") or ""
        pwd2 = request.form.get("password2") or ""

        if not user or not check_password_hash(user["password_hash"], current):
            flash("La contraseña actual no es correcta.", "danger")
            return redirect(url_for("account_password"))

        if len(pwd1) < 6:
            flash("La nueva contraseña debe tener al menos 6 caracteres.", "warning")
            return redirect(url_for("account_password"))

        if pwd1 != pwd2:
            flash("Las nuevas contraseñas no coinciden.", "warning")
            return redirect(url_for("account_password"))

        set_user_password(me, pwd1)
        flash("Contraseña actualizada.", "success")
        return redirect(url_for("home"))

    return render_template("account_password.html")

# ---------------- SCHEDULE (Programación energía) ----------------

DOW_MAP = {0: "L", 1: "M", 2: "X", 3: "J", 4: "V", 5: "S", 6: "D"}

def init_schedule_db():
    conn = db_connect()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                scope TEXT NOT NULL CHECK(scope IN ('all','selected')),
                labs_json TEXT,
                on_days TEXT NOT NULL,
                on_time TEXT NOT NULL,
                off_days TEXT NOT NULL,
                off_time TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                fallback_minutes INTEGER NOT NULL DEFAULT 10,
                created_by TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_on_run TEXT,
                last_off_run TEXT
            )
        """)
        # --- Migración ligera: añadir vcenter_id a schedules si no existe ---
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(schedules)").fetchall()]
        if "vcenter_id" not in cols:
            conn.execute("ALTER TABLE schedules ADD COLUMN vcenter_id TEXT")
            conn.execute("UPDATE schedules SET vcenter_id=? WHERE vcenter_id IS NULL OR vcenter_id=''", (DEFAULT_VCENTER,))


        has_runs = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schedule_runs'"
        ).fetchone()

        if has_runs:
            fk_rows = conn.execute("PRAGMA foreign_key_list(schedule_runs)").fetchall()
            cascade_ok = False
            for r in fk_rows:
                if r[2] == "schedules" and (r[6] or "").upper() == "CASCADE":
                    cascade_ok = True
                    break
            if not cascade_ok:
                conn.execute("DROP TABLE IF EXISTS schedule_runs")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                run_at TEXT NOT NULL,
                action TEXT NOT NULL CHECK(action IN ('on','off')),
                labs_json TEXT,
                ok INTEGER NOT NULL,
                summary TEXT,
                details_json TEXT,
                FOREIGN KEY(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE
            )
        """)
        # --- Migración ligera: añadir vcenter_id a schedule_runs si no existe ---
        cols_r = [r["name"] for r in conn.execute("PRAGMA table_info(schedule_runs)").fetchall()]
        if "vcenter_id" not in cols_r:
            conn.execute("ALTER TABLE schedule_runs ADD COLUMN vcenter_id TEXT")
            conn.execute("UPDATE schedule_runs SET vcenter_id=? WHERE vcenter_id IS NULL OR vcenter_id=''", (DEFAULT_VCENTER,))


        conn.commit()
    finally:
        conn.close()

def _split_days(s: str):
    if not s:
        return []
    return [x for x in s.split(",") if x]

def _now_minute_key(now: datetime) -> str:
    return now.strftime("%Y-%m-%d %H:%M")


def _fetch_schedules(enabled_only=False, vcenter_id=None):
    vcid, _cfg = get_vcenter_cfg(vcenter_id)
    conn = db_connect()
    try:
        if enabled_only:
            cur = conn.execute("SELECT * FROM schedules WHERE enabled=1 AND vcenter_id=? ORDER BY id DESC", (vcid,))
        else:
            cur = conn.execute("SELECT * FROM schedules WHERE vcenter_id=? ORDER BY id DESC", (vcid,))
        return cur.fetchall()
    finally:
        conn.close()


def _fetch_schedule(sched_id: int, vcenter_id=None):
    vcid, _cfg = get_vcenter_cfg(vcenter_id)
    conn = db_connect()
    try:
        cur = conn.execute("SELECT * FROM schedules WHERE id = ? AND vcenter_id = ?", (sched_id, vcid))
        return cur.fetchone()
    finally:
        conn.close()


def _fetch_runs(limit=25, vcenter_id=None):
    vcid, _cfg = get_vcenter_cfg(vcenter_id)
    conn = db_connect()
    try:
        cur = conn.execute("""
            SELECT schedule_id, run_at, action, ok, summary
            FROM schedule_runs
            WHERE vcenter_id=?
            ORDER BY id DESC
            LIMIT ?
        """, (vcid, limit))
        return cur.fetchall()
    finally:
        conn.close()


def _upsert_schedule(form, username: str):
    sched_id = (form.get("id") or "").strip()
    name = (form.get("name") or "").strip()

    # IMPORTANTE: ahora el default en UI es "selected",
    # pero mantenemos el fallback en backend por seguridad
    scope = (form.get("scope") or "selected").strip()

    enabled = 1 if (form.get("enabled") == "on") else 0

    # NUEVO: permitir reglas solo ON / solo OFF
    enable_on = (form.get("enable_on") == "on")
    enable_off = (form.get("enable_off") == "on")

    on_days = form.getlist("on_days") if enable_on else []
    off_days = form.getlist("off_days") if enable_off else []
    on_time = (form.get("on_time") or "").strip() if enable_on else ""
    off_time = (form.get("off_time") or "").strip() if enable_off else ""

    fallback_minutes = int(form.get("fallback_minutes") or "10")

    labs = form.getlist("labs")
    labs_json = json.dumps(labs, ensure_ascii=False)

    if scope not in ("all", "selected"):
        raise ValueError("scope inválido")

    # NUEVO: Confirmación fuerte si scope=all
    if scope == "all":
        confirm_all = (form.get("confirm_all") or "").strip().upper()
        if confirm_all != "TODOS":
            raise ValueError("Para aplicar a TODOS los labs debes confirmar escribiendo: TODOS")

    # Al menos uno activo
    if not enable_on and not enable_off:
        raise ValueError("Debes activar al menos Encendido u Apagado.")

    # Validación específica por bloque
    if enable_on and (not on_days or not on_time):
        raise ValueError("Encendido activo: selecciona días y hora ON.")
    if enable_off and (not off_days or not off_time):
        raise ValueError("Apagado activo: selecciona días y hora OFF.")

    if scope == "selected" and not labs:
        raise ValueError("Si seleccionas 'Seleccionar labs', debes elegir al menos uno.")

    vcid, _cfg = get_vcenter_cfg()
    now = datetime.now().isoformat(timespec="seconds")

    conn = db_connect()
    try:
        if sched_id:
            conn.execute("""
                UPDATE schedules SET
                    name=?, scope=?, labs_json=?,
                    on_days=?, on_time=?, off_days=?, off_time=?,
                    enabled=?, fallback_minutes=?,
                    vcenter_id=?,
                    updated_at=?
                WHERE id=? AND vcenter_id=?
            """, (
                name, scope, labs_json,
                ",".join(on_days), on_time, ",".join(off_days), off_time,
                enabled, fallback_minutes,
                vcid,
                now, int(sched_id), vcid
            ))
        else:
            conn.execute("""
                INSERT INTO schedules
                (vcenter_id, name, scope, labs_json, on_days, on_time, off_days, off_time,
                 enabled, fallback_minutes, created_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                vcid,
                name, scope, labs_json,
                ",".join(on_days), on_time, ",".join(off_days), off_time,
                enabled, fallback_minutes,
                username, now, now
            ))
        conn.commit()
    finally:
        conn.close()


def _toggle_schedule(sched_id: int, vcenter_id=None):
    vcid, _cfg = get_vcenter_cfg(vcenter_id)
    conn = db_connect()
    try:
        row = conn.execute("SELECT enabled FROM schedules WHERE id=? AND vcenter_id=?", (sched_id, vcid)).fetchone()
        if not row:
            return
        new_val = 0 if int(row["enabled"]) == 1 else 1
        conn.execute("UPDATE schedules SET enabled=?, updated_at=? WHERE id=? AND vcenter_id=?",
                     (new_val, datetime.now().isoformat(timespec="seconds"), sched_id, vcid))
        conn.commit()
    finally:
        conn.close()


def _delete_schedule(sched_id: int, vcenter_id=None):
    vcid, _cfg = get_vcenter_cfg(vcenter_id)
    conn = db_connect()
    try:
        conn.execute("DELETE FROM schedule_runs WHERE schedule_id=? AND vcenter_id=?", (sched_id, vcid))
        conn.execute("DELETE FROM schedules WHERE id=? AND vcenter_id=?", (sched_id, vcid))
        conn.commit()
    finally:
        conn.close()


def _log_run(schedule_id: int, action: str, labs: list, ok: bool, summary: str, details: dict, vcenter_id=None):
    vcid, _cfg = get_vcenter_cfg(vcenter_id)
    conn = db_connect()
    try:
        conn.execute("""
            INSERT INTO schedule_runs (vcenter_id, schedule_id, run_at, action, labs_json, ok, summary, details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            vcid,
            schedule_id,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            action,
            json.dumps(labs, ensure_ascii=False),
            1 if ok else 0,
            summary,
            json.dumps(details, ensure_ascii=False)
        ))
        conn.commit()
    finally:
        conn.close()

def _set_last_run(sched_id: int, action: str, minute_key: str):
    col = "last_on_run" if action == "on" else "last_off_run"
    conn = db_connect()
    try:
        conn.execute(f"UPDATE schedules SET {col}=?, updated_at=? WHERE id=?",
                     (minute_key, datetime.now().isoformat(timespec="seconds"), sched_id))
        conn.commit()
    finally:
        conn.close()

def _resolve_labs_for_schedule(content, sched_row):
    scope = sched_row["scope"]
    if scope == "all":
        return list_lab_folders(content, prefix=LAB_FOLDER_PREFIX)
    try:
        return json.loads(sched_row["labs_json"] or "[]")
    except Exception:
        return []

def _lab_power_on_internal(content, lab_name: str):
    folder = find_folder_by_name(content, lab_name)
    if not folder:
        return {"ok": False, "error": "folder not found", "started": 0}

    view = content.viewManager.CreateContainerView(folder, [vim.VirtualMachine], True)
    try:
        to_power = []
        for vm in view.view:
            try:
                if vm.config and vm.config.template:
                    continue
            except Exception:
                pass
            try:
                if str(vm.runtime.powerState) != "poweredOn":
                    to_power.append(vm)
            except Exception:
                to_power.append(vm)

        started = 0
        errors = 0
        for vm in to_power:
            try:
                vm.PowerOnVM_Task()
                started += 1
            except Exception:
                errors += 1

        return {"ok": True, "started": started, "errors": errors}
    finally:
        view.Destroy()

def _lab_power_off_internal(content, lab_name: str, fallback_minutes: int):
    folder = find_folder_by_name(content, lab_name)
    if not folder:
        return {"ok": False, "error": "folder not found", "requested": 0, "forced": 0}

    view = content.viewManager.CreateContainerView(folder, [vim.VirtualMachine], True)
    try:
        vms = []
        for vm in view.view:
            try:
                if vm.config and vm.config.template:
                    continue
            except Exception:
                pass
            vms.append(vm)
    finally:
        view.Destroy()

    requested = 0
    forced = 0
    shutdown_sent = []

    for vm in vms:
        try:
            if str(vm.runtime.powerState) == "poweredOff":
                continue
        except Exception:
            pass

        try:
            vm.ShutdownGuest()
            requested += 1
            shutdown_sent.append(vm)
        except Exception:
            if SOFT_SHUTDOWN_FALLBACK_HARD:
                try:
                    vm.PowerOffVM_Task()
                    forced += 1
                except Exception:
                    pass

    if fallback_minutes and fallback_minutes > 0 and SOFT_SHUTDOWN_FALLBACK_HARD:
        deadline = time.time() + (fallback_minutes * 60)
        while time.time() < deadline:
            still_on = []
            for vm in shutdown_sent:
                try:
                    if str(vm.runtime.powerState) != "poweredOff":
                        still_on.append(vm)
                except Exception:
                    still_on.append(vm)
            if not still_on:
                break
            time.sleep(15)

        for vm in shutdown_sent:
            try:
                if str(vm.runtime.powerState) != "poweredOff":
                    vm.PowerOffVM_Task()
                    forced += 1
            except Exception:
                pass

    return {"ok": True, "requested": requested, "forced": forced}


def process_due_schedules(now: datetime = None):
    """Procesa reglas activas para TODOS los vCenters (pensado para ejecución por cron/daemon)."""
    if now is None:
        now = datetime.now()

    minute_key = _now_minute_key(now)
    day_code = DOW_MAP[now.weekday()]
    hhmm = now.strftime("%H:%M")

    conn = db_connect()
    try:
        schedules = conn.execute("SELECT * FROM schedules WHERE enabled=1 ORDER BY id DESC").fetchall()
    finally:
        conn.close()

    if not schedules:
        return

    for s in schedules:
        on_days = set(_split_days(s["on_days"]))
        off_days = set(_split_days(s["off_days"]))

        vcid = (s["vcenter_id"] or "").strip() or DEFAULT_VCENTER
        if vcid not in VCENTERS:
            vcid = DEFAULT_VCENTER

        if day_code in on_days and hhmm == (s["on_time"] or ""):
            if (s["last_on_run"] or "") != minute_key:
                _run_schedule_action(s, action="on", minute_key=minute_key, vcenter_id=vcid)

        if day_code in off_days and hhmm == (s["off_time"] or ""):
            if (s["last_off_run"] or "") != minute_key:
                _run_schedule_action(s, action="off", minute_key=minute_key, vcenter_id=vcid)


def _run_schedule_action(sched_row, action: str, minute_key: str, vcenter_id=None):
    sched_id = int(sched_row["id"])
    fallback_minutes = int(sched_row["fallback_minutes"] or 0)

    labs = []
    details = {"labs": []}
    ok = True

    try:
        with vcenter_ctx(vcenter_id) as (_, content):
            labs = _resolve_labs_for_schedule(content, sched_row)
            if not labs:
                ok = False
                summary = "Sin labs objetivo (0)."
                _log_run(sched_id, action, labs, ok, summary, details, vcenter_id=vcenter_id)
                _set_last_run(sched_id, action, minute_key)
                return

            for lab_name in labs:
                try:
                    if action == "on":
                        r = _lab_power_on_internal(content, lab_name)
                    else:
                        r = _lab_power_off_internal(content, lab_name, fallback_minutes)

                    if not r.get("ok"):
                        ok = False
                    details["labs"].append({"lab": lab_name, **r})
                except Exception as e:
                    ok = False
                    details["labs"].append({"lab": lab_name, "ok": False, "error": str(e)})

        if action == "on":
            started = sum(x.get("started", 0) for x in details["labs"])
            summary = f"ON: labs={len(labs)} · VMs encendidas solicitadas={started}"
        else:
            req = sum(x.get("requested", 0) for x in details["labs"])
            forced = sum(x.get("forced", 0) for x in details["labs"])
            summary = f"OFF: labs={len(labs)} · shutdown={req} · forced={forced}"

        _log_run(sched_id, action, labs, ok, summary, details, vcenter_id=vcenter_id)

    except Exception as e:
        ok = False
        summary = f"ERROR vCenter: {e}"
        _log_run(sched_id, action, labs, ok, summary, details, vcenter_id=vcenter_id)

    finally:
        _set_last_run(sched_id, action, minute_key)

@app.get("/schedule")
@login_required
@role_required("admin")
def schedule_page():
    labs_data = []
    try:
        with vcenter_ctx() as (_, content):
            lab_names = list_lab_folders(content, prefix=LAB_FOLDER_PREFIX)
            for name in lab_names:
                try:
                    folder = find_folder_by_name(content, name)
                    c = count_vms_in_folder(content, folder) if folder else 0
                except Exception:
                    c = 0
                labs_data.append({"name": name, "count": c})
    except Exception:
        labs_data = []

    schedules = []
    for row in _fetch_schedules(enabled_only=False):
        try:
            labs = json.loads(row["labs_json"] or "[]")
        except Exception:
            labs = []
        schedules.append({
            "id": row["id"],
            "name": row["name"],
            "scope": row["scope"],
            "labs": labs,
            "on_days": _split_days(row["on_days"]),
            "on_time": row["on_time"],
            "off_days": _split_days(row["off_days"]),
            "off_time": row["off_time"],
            "enabled": bool(int(row["enabled"])),
            "fallback_minutes": int(row["fallback_minutes"] or 0),
            "last_on_run": row["last_on_run"],
            "last_off_run": row["last_off_run"],
        })

    runs = _fetch_runs(limit=25)

    edit_id = request.args.get("edit", "").strip()
    edit = None
    if edit_id.isdigit():
        r = _fetch_schedule(int(edit_id))
        if r:
            try:
                labs = json.loads(r["labs_json"] or "[]")
            except Exception:
                labs = []
            edit = {
                "id": r["id"],
                "name": r["name"],
                "scope": r["scope"],
                "labs": labs,
                "on_days": _split_days(r["on_days"]),
                "on_time": r["on_time"],
                "off_days": _split_days(r["off_days"]),
                "off_time": r["off_time"],
                "enabled": bool(int(r["enabled"])),
                "fallback_minutes": int(r["fallback_minutes"] or 0),
                "last_on_run": row["last_on_run"],
                "last_off_run": row["last_off_run"],

            }

    return render_template("schedule.html",
                           prefix=LAB_FOLDER_PREFIX,
                           labs=labs_data,
                           schedules=schedules,
                           runs=runs,
                           edit=edit)

@app.post("/schedule/save")
@login_required
@role_required("admin")
def schedule_save():
    try:
        _upsert_schedule(request.form, session.get("username") or "admin")
        flash("Programación guardada.", "success")
    except Exception as e:
        flash(f"Error guardando programación: {e}", "danger")
    return redirect(url_for("schedule_page"))

@app.post("/schedule/<int:sched_id>/toggle")
@login_required
@role_required("admin")
def schedule_toggle(sched_id):
    _toggle_schedule(sched_id)
    flash("Regla actualizada.", "success")
    return redirect(url_for("schedule_page"))

@app.post("/schedule/<int:sched_id>/delete")
@login_required
@role_required("admin")
def schedule_delete(sched_id):
    confirm = (request.form.get("confirm") or "").strip().upper()
    if confirm != "SI":
        flash("Confirmación inválida para eliminar regla.", "danger")
        return redirect(url_for("schedule_page"))

    _delete_schedule(sched_id)
    flash("Regla eliminada.", "success")
    return redirect(url_for("schedule_page"))

@app.post("/schedule/<int:sched_id>/run/<action>")
@login_required
@role_required("admin")
def schedule_run_now(sched_id, action):
    if action not in ("on", "off"):
        flash("Acción inválida.", "danger")
        return redirect(url_for("schedule_page"))

    s = _fetch_schedule(sched_id)
    if not s:
        flash("Regla no encontrada.", "danger")
        return redirect(url_for("schedule_page"))

    minute_key = _now_minute_key(datetime.now())
    _run_schedule_action(s, action=action, minute_key=minute_key)
    flash(f"Ejecutado {action.upper()} manualmente. Revisa 'Ejecuciones recientes'.", "success")
    return redirect(url_for("schedule_page"))

init_user_db()
init_schedule_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("FLASK_PORT", "5000")), debug=False)
