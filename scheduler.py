#!/usr/bin/env python3
import os
import time
import fcntl
import inspect
from datetime import datetime
from dotenv import load_dotenv

# Intenta leer .env del directorio actual si existe (en Docker suele venir por env_file)
load_dotenv()

# Aplica TZ (si existe)
try:
    time.tzset()
except Exception:
    pass

import app  # tu app.py

LOCK_PATH = os.getenv("VMLABS_SCHED_LOCK", "/tmp/vmlabs_scheduler.lock")
SLEEP_SEC = int(os.getenv("VMLABS_SCHED_SLEEP_SEC", "20"))
TZ = os.getenv("TZ", "Europe/Madrid")

# Control de verbosidad por .env
VERBOSE = os.getenv("VMLABS_SCHED_VERBOSE", "0").strip().lower() in ("1", "true", "yes", "on", "y")

DOW_MAP = {0: "L", 1: "M", 2: "X", 3: "J", 4: "V", 5: "S", 6: "D"}


def acquire_lock():
    f = open(LOCK_PATH, "w")
    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return f


def _split_days(s: str):
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _minute_key(now: datetime) -> str:
    return now.strftime("%Y-%m-%d %H:%M")


def _run_schedule_action(row, action: str, minute_key: str, vcenter_id: str | None):
    """Compat: llama a la función interna con o sin vcenter_id."""
    fn = getattr(app, "_run_schedule_action", None)
    if not fn:
        # fallback
        return app.process_due_schedules(now=datetime.now())

    sig = inspect.signature(fn)
    try:
        if "vcenter_id" in sig.parameters:
            return fn(row, action=action, minute_key=minute_key, vcenter_id=vcenter_id)
        return fn(row, action=action, minute_key=minute_key)
    except TypeError:
        return fn(row, action, minute_key)


def _process_due_lab_deletions(now: datetime):
    """
    Hook para borrados programados.
    Se ejecuta solo si app.py expone process_due_lab_deletions().
    """
    fn = getattr(app, "process_due_lab_deletions", None)
    if not fn:
        return
    try:
        fn(now=now)
    except TypeError:
        # Por si tu firma es distinta
        fn(now)
    except Exception as e:
        print("[scheduler] lab deletions ERROR:", repr(e), flush=True)


def main():
    # Asegura tablas schedule
    try:
        app.init_schedule_db()
    except Exception as e:
        print("[scheduler] init_schedule_db ERROR:", e, flush=True)

    # NUEVO: Asegura tablas de borrados programados (si está implementado en app.py)
    try:
        init_del = getattr(app, "init_lab_deletions_db", None)
        if init_del:
            init_del()
    except Exception as e:
        print("[scheduler] init_lab_deletions_db ERROR:", e, flush=True)

    try:
        _lock_file = acquire_lock()
    except BlockingIOError:
        print("[scheduler] another instance already holds lock:", LOCK_PATH, flush=True)
        return

    print("[scheduler] started, lock acquired:", LOCK_PATH, "TZ:", TZ, flush=True)

    last_minute = None

    while True:
        now = datetime.now()
        minute_key = _minute_key(now)

        if minute_key != last_minute:
            last_minute = minute_key
            day_code = DOW_MAP[now.weekday()]
            hhmm = now.strftime("%H:%M")

            try:
                conn = app.db_connect()
                try:
                    schedules = conn.execute("SELECT * FROM schedules WHERE enabled=1 ORDER BY id DESC").fetchall()
                finally:
                    conn.close()

                if not schedules:
                    if VERBOSE:
                        print(f"[scheduler] {minute_key} no enabled schedules", flush=True)
                else:
                    due = 0
                    for s in schedules:
                        on_days = set(_split_days(s["on_days"] if "on_days" in s.keys() else ""))
                        off_days = set(_split_days(s["off_days"] if "off_days" in s.keys() else ""))
                        on_time = (s["on_time"] if "on_time" in s.keys() else "") or ""
                        off_time = (s["off_time"] if "off_time" in s.keys() else "") or ""

                        vcid = (s["vcenter_id"] or "").strip() if ("vcenter_id" in s.keys()) else ""
                        vcid = vcid or None

                        # ON
                        if day_code in on_days and hhmm == on_time:
                            last = (s["last_on_run"] or "") if "last_on_run" in s.keys() else ""
                            if last != minute_key:
                                due += 1
                                print(f"[scheduler] {minute_key} RUN ON schedule_id={s['id']} vcenter={vcid}", flush=True)
                                _run_schedule_action(s, "on", minute_key, vcid)

                        # OFF
                        if day_code in off_days and hhmm == off_time:
                            last = (s["last_off_run"] or "") if "last_off_run" in s.keys() else ""
                            if last != minute_key:
                                due += 1
                                print(f"[scheduler] {minute_key} RUN OFF schedule_id={s['id']} vcenter={vcid}", flush=True)
                                _run_schedule_action(s, "off", minute_key, vcid)

                    if due == 0 and VERBOSE:
                        print(f"[scheduler] {minute_key} tick ok (no due rules)", flush=True)

                # NUEVO: ejecutar borrados programados (one-shot) si existen
                _process_due_lab_deletions(now)

            except Exception as e:
                print("[scheduler] ERROR:", repr(e), flush=True)

        time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()