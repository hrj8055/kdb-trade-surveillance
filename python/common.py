# python/common.py
# Shared by every Python process: config, UTC logging, and q connections.

import json
import logging
import sys
import time
from pathlib import Path

import pykx as kx

ROOT = Path(__file__).resolve().parent.parent
CFG = json.loads((ROOT / "config" / "config.json").read_text())


def get_logger(name):
    """Timestamped, levelled, UTC logs: same clock as the tickerplant."""
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)sZ %(levelname)s [%(name)s] %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S")
    return logging.getLogger(name)


def connect(proc, log):
    """Open a connection to a q process by its name in config.json (tp, rdb, ...)."""
    port = CFG["ports"][proc]
    try:
        return kx.SyncQConnection(host=CFG["host"], port=port)
    except kx.QError:
        log.error("Cannot reach %s on port %s. Start it first.", proc, port)
        sys.exit(1)
