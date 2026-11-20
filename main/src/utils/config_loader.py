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
    if not os.pa