import logging
import os
from logging.handlers import RotatingFileHandler

from config import LOG_BACKUP_COUNT, LOG_FILE, LOG_LEVEL, LOG_MAX_BYTES, RUNTIME_LOG_FILE


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

