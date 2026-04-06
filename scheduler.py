#!/usr/bin/env python3
import os
import time
import fcntl
import inspect
import logging
import sys
from datetime import datetime
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Aplica TZ (si existe)
try:
    time.tzset()
except Exception as e:
    logger.warning(f"No se pudo establecer TZ: {e}")

import app

LOCK_PATH = os.getenv("VMLABS_SCHED_LOCK", "/tmp/vmlabs_scheduler.lock")
SLEEP_SEC = int(os.getenv("VMLABS_SCHED_SLEEP_SEC", "20"))
TZ = os.getenv("TZ", "Europe/Madrid")

VERBOSE = os.getenv("VMLABS_SCHED_VERBOSE", "0").strip().lower() in ("1", "true", "yes", "on", "y")

DOW_MAP = {0: "L", 1: "M", 2: "X", 3: "J", 4: "V", 5: "S", 6: "D"}

def acquire_lock():
    """Adquirir lock para evitar múltiples instancias del scheduler"""
    try:
        lock_dir = os.path.dirname(LOCK_PATH)
        if lock_dir and not os.path.exists(lock_dir):
            os.makedirs(lock_dir, exist_ok=True)
        
        f = open(LOCK_PATH, "w")
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return f
    except BlockingIOError:
        logger.warning(f"Otra instancia ya tiene el lock: {LOCK_PATH}")
        return None
    except Exception as e:
        logger.error(f"Error al adquirir lock: {e}")
        return None

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
        logger.warning("_run_schedule_action no encontrado en app")
        return app.process_due_schedules(now=datetime.now())
    
    sig = inspect.signature(fn)
    try:
        if "vcenter_id" in sig.parameters:
            return fn(row, action=action, minute_key=minute_key, vcenter_id=vcenter_id)
        return fn(row, action=action, minute_key=minute_key)
    except TypeError as e:
        logger.error(f"Error llamando _run_schedule_action: {e}")
        return fn(row, action, minute_key)

def _process_due_lab_deletions(now: datetime):
    """Hook para borrados programados."""
    fn = getattr(app, "process_due_lab_deletions", None)
    if not fn:
        return
    try:
        fn(now=now)
    except TypeError:
        fn(now)
    except Exception as e:
        logger.error(f"lab deletions ERROR: {e}")

def main():
    logger.info(f"Iniciando scheduler (TZ={TZ}, verbose={VERBOSE}, PID={os.getpid()})")
    
    # Asegurar tablas
    try:
        app.init_schedule_db()
        logger.info("Tabla schedule inicializada")
    except Exception as e:
        logger.error(f"init_schedule_db ERROR: {e}")
    
    try:
        init_del = getattr(app, "init_lab_deletions_db", None)
        if init_del:
            init_del()
            logger.info("Tabla lab_deletions inicializada")
    except Exception as e:
        logger.error(f"init_lab_deletions_db ERROR: {e}")
    
    # Adquirir lock
    lock_file = acquire_lock()
    if lock_file is None:
        logger.error("No se pudo adquirir lock. Saliendo...")
        sys.exit(1)
    
    logger.info(f"Lock adquirido: {LOCK_PATH}")
    
    last_minute = None
    
    try:
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
                            logger.debug(f"{minute_key} no enabled schedules")
                    else:
                        due = 0
                        for s in schedules:
                            # ACCESO CORRECTO: usar corchetes en lugar de .get()
                            on_days_str = s["on_days"] if "on_days" in s.keys() else ""
                            off_days_str = s["off_days"] if "off_days" in s.keys() else ""
                            on_time = s["on_time"] if "on_time" in s.keys() else ""
                            off_time = s["off_time"] if "off_time" in s.keys() else ""
                            
                            on_days = set(_split_days(on_days_str))
                            off_days = set(_split_days(off_days_str))
                            
                            vcid = (s["vcenter_id"] or "").strip() if "vcenter_id" in s.keys() else None
                            
                            # ON
                            if day_code in on_days and hhmm == on_time:
                                last = s["last_on_run"] if "last_on_run" in s.keys() else ""
                                if last != minute_key:
                                    due += 1
                                    logger.info(f"{minute_key} RUN ON schedule_id={s['id']} vcenter={vcid}")
                                    _run_schedule_action(s, "on", minute_key, vcid)
                            
                            # OFF
                            if day_code in off_days and hhmm == off_time:
                                last = s["last_off_run"] if "last_off_run" in s.keys() else ""
                                if last != minute_key:
                                    due += 1
                                    logger.info(f"{minute_key} RUN OFF schedule_id={s['id']} vcenter={vcid}")
                                    _run_schedule_action(s, "off", minute_key, vcid)
                        
                        if due == 0 and VERBOSE:
                            logger.debug(f"{minute_key} tick ok (no due rules)")
                    
                    _process_due_lab_deletions(now)
                    
                except Exception as e:
                    logger.error(f"Scheduler error: {e}", exc_info=True)
            
            time.sleep(SLEEP_SEC)
            
    except KeyboardInterrupt:
        logger.info("Scheduler detenido por señal")
    finally:
        if lock_file:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()
                logger.info("Lock liberado")
            except Exception as e:
                logger.error(f"Error liberando lock: {e}")

if __name__ == "__main__":
    main()
