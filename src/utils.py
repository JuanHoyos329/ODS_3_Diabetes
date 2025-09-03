# Utility functions for logging and file operations

import logging
import os
from datetime import datetime

def setup_logging(level: str = 'INFO', log_file: str = None):
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    if log_file:
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    else:
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

def ensure_directory_exists(directory_path: str):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Created directory: {directory_path}")
    else:
        logging.debug(f"Directory already exists: {directory_path}")

def get_timestamp_string() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")