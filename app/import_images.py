import logging
import uuid
from pathlib import Path
from typing import Iterable

from app.db.connection import get_connection


IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff"}


def iter_images(folder: Path) -> Iterable[Path]:
    if not folder.exists():
        return []
    for p in folder.rglob("*"):
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
            yield p


def import_images(db_path: Path, raw_images_dir: Path) -> int:
    """
    Импортирует изображения в таблицу images.
    Возвращает число реально добавленных записей.
    """
    added = 0
    raw_images_dir = raw_images_dir.resolve()

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        for img_path in iter_images(raw_images_dir):
            image_id = str(uuid.uuid4())
            path_str = str(img_path)

            try:
                cur.execute(
                    """
                    INSERT INTO images (image_id, path)
                    VALUES (?, ?)
                    """,
                    (image_id, path_str),
                )
                added += 1
                logging.info("Imported: %s", path_str)
            except Exception as e:
                # чаще всего это UNIQUE constraint failed: images.path
                logging.warning("Skipped (maybe duplicate): %s | %s", path_str, e)

        conn.commit()

    return added
