# utils.py

import logging
import os
from datetime import datetime
from typing import Optional


def setup_logging(level: str = 'INFO', log_file: Optional[str] = None) -> None:
    logger = logging.getLogger()
    # Normaliza el nivel
    log_level = getattr(logging, str(level).upper(), logging.INFO)
    logger.setLevel(log_level)

    # Si ya tiene handlers configurados con el mismo destino, no duplicar
    existing_types = {type(h) for h in logger.handlers}

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Archivo + consola
    if log_file:
        if logging.FileHandler not in existing_types:
            fh = logging.FileHandler(log_file)
            fh.setLevel(log_level)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        if logging.StreamHandler not in existing_types:
            sh = logging.StreamHandler()
            sh.setLevel(log_level)
            sh.setFormatter(formatter)
            logger.addHandler(sh)
    else:
        # Solo consola
        if logging.StreamHandler not in existing_types:
            sh = logging.StreamHandler()
            sh.setLevel(log_level)
            sh.setFormatter(formatter)
            logger.addHandler(sh)


def ensure_directory_exists(directory_path: str) -> None:

    created = False
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)
        created = True

    if created:
        logging.info(f"Created directory: {directory_path}")
    else:
        logging.debug(f"Directory already exists: {directory_path}")


def get_timestamp_string() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")