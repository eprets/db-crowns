from pathlib import Path
import yaml


def load_config(path: str):
    """
    Загружает YAML-конфиг и возвращает словарь
    """
    config_path = Path(path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    with config_path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config
