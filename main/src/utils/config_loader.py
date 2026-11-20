import os
from pathlib import Path
import yaml


from urllib.parse import unquote


def to_uri_path(path_str: str) -> str:
    """Converts a local file path to a URI representation compatible with Apache Hudi on Windows and POSIX."""
    abs_p = os.path.abspath(path_str)
    return unquote(Path(abs_p).as_uri())


def load_config(config_path: str) -> dict:
    """Loads YAML configuration and resolves relative paths against the project root."""
    if not os.path.isabs(config_path):
        config_path = os.path.abspath(config_path)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Detect project root directory from config location
    config_dir = os.path.dirname(config_path)
    if os.path.basename(config_dir) == "configs":
        base_dir = os.path.dirname(config_dir)
    else:
        base_dir = os.getcwd()

    for section in ["raw", "bronze", "quarantine", "silver", "gold"]:
        if section in config.get("paths", {}):
            for key, rel_path in config["paths"][section].items():
                if not os.path.isabs(rel_path):
                    config["paths"][section][key] = os.path.abspath(
                        os.path.join(base_dir, rel_path)
                    )
    return config
