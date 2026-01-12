import logging
import os
import time
from logging.handlers import RotatingFileHandler

from config import (
    LOG_BACKUP_COUNT,
    LOG_FILE,
    LOG_LEVEL,
    LOG_MAX_BYTES,
    LOG_RETENTION_DAYS,
    RUNTIME_LOG_FILE,
)


def cleanup_old_logs(retention_days: int | None = None) -> int:
    keep_days = LOG_RETENTION_DAYS if retention_days is None else int(retention_days)
    if keep_days <= 0:
        return 0

    cutoff = time.time() - (keep_days * 86400)
    removed = 0

    targets = [os.path.abspath(LOG_FILE), os.path.abspath(RUNTIME_LOG_FILE)]
    for current_path in targets:
        log_dir = os.path.dirname(current_path) or "."
        base = os.path.basename(current_path)
        try:
            names = os.listdir(log_dir)
        except Exception:
            continue

        for name in names:
            if name == base:
                continue
            if not name.startswith(base):
                continue
            fp = os.path.join(log_dir, name)
            try:
                if not os.path.isfile(fp):
                    continue
                if os.path.getmtime(fp) >= cutoff:
                    continue
                os.remove(fp)
                removed += 1
            except Exception:
                continue

    return removed


def setup_logging():
    os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(RUNTIME_LOG_FILE) or ".", exist_ok=True)

    root = logging.getLogger()
    if getattr(root, "_pv_logging_configured", False):
        return

    level = getattr(logging, LOG_LEVEL, logging.INFO)
    root.setLevel(level)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s | %(message)s")

    bot_fh = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    bot_fh.setFormatter(fmt)
    bot_fh.setLevel(level)

    rt_fh = RotatingFileHandler(
        RUNTIME_LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    rt_fh.setFormatter(fmt)
    rt_fh.setLevel(level)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(level)

    root.addHandler(bot_fh)
    root.addHandler(rt_fh)
    root.addHandler(sh)
    root._pv_logging_configured = True
    try:
        cleanup_old_logs()
    except Exception:
        pass

