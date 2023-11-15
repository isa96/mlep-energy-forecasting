import json
import logging
from pathlib import Path
from .settings import OUTPUT_DIR


def get_logger(name: str) -> logging.Logger:
    """
    Template get a logger
    Args:
        name: name of the logger
    return: Logger
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name)
    return logger

def save_json(data: dict, file_name: str, save_dir: str = OUTPUT_DIR):
    """
    Save a dictionary as a JSON file.

    Args:
        data: data to save.
        file_name: Name of the JSON file.
        save_dir: Directory to save the JSON file.
    """
    data_path = Path(save_dir) / file_name
    with open(data_path, "w") as f:
        json.dump(data, f)

def load_json(file_name: str, save_dir: str = OUTPUT_DIR) -> dict:
    """
    Load a JSON file.

    Args:
        file_name: Name of the JSON file.
        save_dir: Directory of the JSON file.

    Returns: Dictionary with the data.
    """

    data_path = Path(save_dir) / file_name
    if not data_path.exists():
        raise FileNotFoundError(f"Cached JSON from {data_path} does not exist.")

    with open(data_path, "r") as f:
        return json.load(f)