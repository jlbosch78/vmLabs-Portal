# VM Labs Portal

Portal web (Flask) para administrar laboratorios (carpetas) y máquinas virtuales en **VMware vCenter**, con:

- Soporte **multi‑vCenter** (el admin puede elegir el vCenter activo desde la UI; el rol profesor puede quedar “fijado” a un vCenter).
- Creación de labs clonando VMs desde un **template**, con **distribución por hosts** a partir de un CSV.
- Acciones de energía por VM y por lab (ON/OFF/Reboot).
- Snapshots: crear, borrar el último, revertir a baseline (“recrear”).
- **Scheduler** semanal para encendido/apagado automático por vCenter.
- Usuarios/roles locales (admin/profesor) almacenados en SQLite.


---

## Requisitos

- Docker
- Docker Compose  
  - `docker compose` (plugin v2) **o** `docker-compose` (clásico v1)

---

## Puesta en marcha rápida (Docker)

### 1) Clonar
```bash
git clone <URL_DE_TU_REPO_EN_GITHUB>
cd <carpeta-del-repo>
```

### 2) Crear `.env`
```bash
cp .env.example .env
nano .env
```

Configura como mínimo:
- `VCENTERS_JSON` (hosts/usuarios/contraseñas reales de vCenter)
- `FLASK_SECRET` (cadena larga aleatoria)
- `AUTH_PASS` (solo se usa para crear el admin inicial si la BD está vacía)

> **No comitees nunca `.env`**.

### 3) Crear carpetas persistentes
```bash
mkdir -p data tmp
```

- `./data` persiste la base de datos SQLite (`users.db`).
- `./tmp` guarda temporales de jobs/CSVs.

### 4) Arrancar
Compose v2:
```bash
docker compose up -d --build
```

Compose v1:
```bash
docker-compose up -d --build
```

Abrir:
- http://localhost:5000 (o `http://IP_DEL_SERVIDOR:5000`)

Logs:
```bash
docker-compose logs -f web
docker-compose logs -f scheduler
```

---

## Servicios

Docker Compose levanta dos contenedores:

- **web**: Gunicorn + Flask en el puerto **5000**
- **scheduler**: proceso que revisa reglas cada minuto y ejecuta ON/OFF

Ambos contenedores comparten:
- `./data:/data` (persistencia SQLite)
- `./tmp:/tmp/vclab_jobs` (temporales)
- zona horaria del host vía `/etc/localtime` y `/etc/timezone` (recomendado)

---

## Referencia de configuración (`.env`)

### Multi‑vCenter: `VCENTERS_JSON`

Ejemplo (ver `.env.example`):

- `label`: nombre amigable en la UI
- `host`: host/IP del vCenter
- `user`, `pass`: credenciales
- `verify_ssl`: `true/false`

Comportamiento:
- `DEFAULT_VCENTER`: vCenter por defecto (fallback)
- `PROF_VCENTER`: vCenter usado por el rol `profesor`

### Login del portal (local)

- `AUTH_USER`, `AUTH_PASS`: se usan **solo** para crear el usuario admin inicial **si** `users.db` está vacío.
- `FLASK_SECRET`: secreto de sesión de Flask (debe ser fuerte en producción).
- `FLASK_PORT`: por defecto `5000`.

### Persistencia / rutas

- `USER_DB_PATH=/data/users.db`  (persistido en el host vía `./data`)
- `TMP_DIR=/tmp/vclab_jobs`       (persistido en el host vía `./tmp`)
- `POLL_INTERVAL_MS`             (intervalo de refresco de estado de VMs)

### Logs del scheduler

- `VMLABS_SCHED_VERBOSE=0` → solo `RUN/ERROR`
- `VMLABS_SCHED_VERBOSE=1` → también ticks/debug (si está implementado en `scheduler.py`)

### Zona horaria

Define:
- `TZ=Europe/Madrid` (o tu zona horaria)

Si la zona horaria no coincide, las reglas automáticas no cuadrarán con el `HH:MM` configurado.

---

## Roles y modelo de acceso

- **admin**
  - Puede seleccionar el vCenter activo.
  - Puede crear labs, gestionar usuarios y configurar schedules.

- **profesor**
  - Puede quedar fijado a `PROF_VCENTER`.
  - Solo ve/gestiona labs asignados por el admin.

---

## Creación de labs (clonado)

Flujo típico:
1) El admin elige template, carpeta destino, datastore, red y sube un CSV con usuarios (uno por línea).
2) El portal planifica la distribución por hosts y arranca un job.
3) El progreso se muestra en tiempo real (SSE `/stream/<job_id>`).

Detalles:
- Nombres de VM: `<folder_name>-<username>`
- La NIC se reconfigura a la red/portgroup seleccionada.
- Snapshot baseline opcional tras clonar.
- VM opcional “profesor” clonada en carpeta `Profesores`.

---

## Scheduler (encendido/apagado automático)

- Las reglas se configuran en la UI (`/schedule`).
- Alcance (scope):
  - **Selected labs** (por defecto seguro)
  - **All labs** (requiere confirmación explícita para evitar accidentes)
- Permite “solo ON”, “solo OFF” o ambos.

El contenedor `scheduler` ejecuta reglas cada minuto y registra ejecuciones para auditoría.

---

## Reverse proxy (opcional)

Si pones el portal detrás de Nginx / Nginx Proxy Manager y usas el stream en vivo (`/stream/...`),
desactiva el buffering en esa ruta (SSE):

```nginx
location /stream/ {
  proxy_buffering off;
  proxy_cache off;
  proxy_read_timeout 3600;
  proxy_send_timeout 3600;
}
```

---

## Notas de seguridad

- No comitees `.env`.
- No comitees `./data/users.db`.
- Usa credenciales de vCenter con privilegios mínimos.
- Usa un `FLASK_SECRET` fuerte.

---

## Troubleshooting

### Las reglas programadas no se ejecutan automáticamente
1) Comprueba la hora dentro del contenedor:
```bash
docker-compose exec scheduler date
```
2) Asegura que ambos contenedores comparten el mismo volumen de BD (`./data:/data`) y `USER_DB_PATH=/data/users.db`.
3) Verifica que la regla está **habilitada** en la UI.

### `docker compose` no existe
En algunos Debian antiguos se usa `docker-compose` en vez de `docker compose`.

---

## Licencia

Este proyecto se publica bajo licencia **MIT**. Ver `LICENSE`.
