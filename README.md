# VM Labs Portal

A small web portal (Flask) to manage **VMware vCenter** labs (folders) and virtual machines, with:

- **Multi‑vCenter** support (admin can choose the active vCenter from the UI; professors can be pinned to one vCenter).
- Lab creation by cloning VMs from a **template**, including **host distribution** from a CSV list.
- VM and lab power actions (ON/OFF/Reboot).
- Snapshots: create, delete latest, revert to baseline (“recreate”).
- Weekly **scheduler** for automatic power ON/OFF per vCenter.
- Local users/roles (admin/professor) stored in SQLite.

> This project **does not assign vCenter permissions** during cloning (permission assignment was removed/disabled).

---

## Requirements

- Docker
- Docker Compose  
  - Either `docker compose` (plugin v2) **or** `docker-compose` (classic v1)

---

## Quick start (Docker)

### 1) Clone
```bash
git clone <YOUR_GITHUB_REPO_URL>
cd <repo-folder>
```

### 2) Create `.env`
```bash
cp .env.example .env
nano .env
```

Configure at least:
- `VCENTERS_JSON` (real vCenter hosts/users/passwords)
- `FLASK_SECRET` (long random string)
- `AUTH_PASS` (used only to create the initial admin if the DB is empty)

> **Never commit `.env`**.

### 3) Create persistent folders
```bash
mkdir -p data tmp
```

- `./data` persists the SQLite database (`users.db`).
- `./tmp` stores temporary CSV/job artifacts.

### 4) Start
Compose v2:
```bash
docker compose up -d --build
```

Compose v1:
```bash
docker-compose up -d --build
```

Open:
- http://localhost:5000  (or `http://SERVER_IP:5000`)

Logs:
```bash
docker-compose logs -f web
docker-compose logs -f scheduler
```

---

## Services

Docker Compose runs two containers:

- **web**: Gunicorn + Flask app on port **5000**
- **scheduler**: a small process that checks schedules every minute and triggers ON/OFF

Both containers share:
- `./data:/data` (SQLite persistence)
- `./tmp:/tmp/vclab_jobs` (temporary job files)
- host timezone via `/etc/localtime` and `/etc/timezone` (recommended)

---

## Configuration reference (`.env`)

### Multi‑vCenter: `VCENTERS_JSON`

Example (see `.env.example`):

- `label`: human friendly name shown in the UI
- `host`: vCenter host/IP
- `user`, `pass`: credentials
- `verify_ssl`: `true/false`

Selection behavior:
- `DEFAULT_VCENTER`: fallback default
- `PROF_VCENTER`: vCenter used by the `profesor` role

### Portal login (local)

- `AUTH_USER`, `AUTH_PASS`: used **only** to create the initial admin account **if** `users.db` is empty.
- `FLASK_SECRET`: Flask session secret (must be strong in production).
- `FLASK_PORT`: defaults to `5000`.

### Persistence / paths

- `USER_DB_PATH=/data/users.db`  (persisted on the host via `./data`)
- `TMP_DIR=/tmp/vclab_jobs`       (persisted on the host via `./tmp`)
- `POLL_INTERVAL_MS`             (VM status refresh interval)

### Scheduler logs

- `VMLABS_SCHED_VERBOSE=0` → only logs RUN/ERROR
- `VMLABS_SCHED_VERBOSE=1` → also logs ticks/debug (if enabled in `scheduler.py`)

### Timezone

Set:
- `TZ=Europe/Madrid` (or your timezone)

If timezone is wrong, automatic schedules will not match the configured `HH:MM`.

---

## Roles & access model

- **admin**
  - Can select active vCenter.
  - Can create labs, manage users, configure schedules.

- **profesor**
  - Can be pinned to `PROF_VCENTER`.
  - Can see/manage only labs assigned by the admin.

---

## Lab creation (cloning)

Typical flow:
1) Admin selects template, destination folder, datastore, network and uploads a CSV with usernames (one per row).
2) The portal plans host distribution and starts a background job.
3) Progress is streamed live (SSE `/stream/<job_id>`).

Cloning details:
- VM names are generated as: `<folder_name>-<username>`
- NIC is reconfigured to the selected network/portgroup.
- Optional baseline snapshot after cloning.
- Optional “teacher VM” cloned into a `Profesores` folder.

---

## Scheduler (automatic ON/OFF)

- Rules are configured from the UI (`/schedule`).
- Scope:
  - **Selected labs** (safe default)
  - **All labs** (requires explicit confirmation to prevent accidents)
- Supports “only ON”, “only OFF” or both.

The scheduler container executes rules every minute and records runs for auditing.

---

## Reverse proxy (optional)

If you put the portal behind Nginx / Nginx Proxy Manager and you use the live job stream (`/stream/...`),
disable proxy buffering for that path (SSE):

```nginx
location /stream/ {
  proxy_buffering off;
  proxy_cache off;
  proxy_read_timeout 3600;
  proxy_send_timeout 3600;
}
```

---

## Security notes

- Do not commit `.env`.
- Do not commit `./data/users.db`.
- Use least‑privilege vCenter credentials.
- Set a strong `FLASK_SECRET`.

---

## Troubleshooting

### Schedules do not execute automatically
1) Check container time:
```bash
docker-compose exec scheduler date
```
2) Ensure both containers share the same DB volume (`./data:/data`) and `USER_DB_PATH=/data/users.db`.
3) Verify the rule is **enabled** in the UI.

### `docker compose` not found
Use `docker-compose` on older Debian installations.

---

## License

This project is released under the **MIT License**. See `LICENSE`.
