# app/build_ellipse_masks.py
import logging
import math
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

from app.db.connection import get_connection


def read_image_unicode(path: str) -> Optional[np.ndarray]:
    """
    Безопасно читает изображение по пути с русскими буквами.
    """
    data = np.fromfile(path, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    return img


def save_image_unicode(path: Path, img: np.ndarray) -> bool:
    """
    Безопасно сохраняет изображение по пути с русскими буквами.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    ok, buf = cv2.imencode(".png", img)
    if not ok:
        return False

    buf.tofile(str(path))
    return True


def build_ellipse_masks(
    db_path: Path,
    masks_dir: Path,
    overwrite: bool = False,
) -> int:
    """
    Создаёт маски кроны по параметрам эллипса из annotations.

    Для каждой annotation:
    - открывает исходное изображение;
    - создаёт mask размера изображения;
    - рисует белый эллипс;
    - сохраняет mask_path;
    - обновляет annotations.mask_path.

    Возвращает количество созданных/обновлённых масок.
    """

    masks_dir.mkdir(parents=True, exist_ok=True)
    created = 0

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                a.annotation_id,
                a.image_id,
                a.tree_id,
                a.x0,
                a.y0,
                a.a,
                a.b,
                a.theta,
                a.mask_path,
                i.path AS image_path
            FROM annotations a
            JOIN images i ON i.image_id = a.image_id
            ORDER BY a.created_at ASC
            """
        )

        rows = cur.fetchall()

        for r in rows:
            annotation_id = r["annotation_id"]
            old_mask_path = r["mask_path"]

            if old_mask_path and not overwrite:
                logging.info("Skip mask exists: annotation_id=%s", annotation_id)
                continue

            img = read_image_unicode(r["image_path"])

            if img is None:
                logging.warning("Cannot read image: %s", r["image_path"])
                continue

            h, w = img.shape[:2]

            mask = np.zeros((h, w), dtype=np.uint8)

            center = (int(round(float(r["x0"]))), int(round(float(r["y0"]))))
            axes = (int(round(float(r["a"]))), int(round(float(r["b"]))))
            angle_deg = float(r["theta"]) * 180.0 / math.pi

            cv2.ellipse(
                mask,
                center,
                axes,
                angle_deg,
                0,
                360,
                255,
                thickness=-1,
            )

            mask_path = masks_dir / f"{annotation_id}.png"

            ok = save_image_unicode(mask_path, mask)

            if not ok:
                logging.warning("Cannot save mask: %s", mask_path)
                continue

            cur.execute(
                """
                UPDATE annotations
                SET mask_path = ?
                WHERE annotation_id = ?
                """,
                (str(mask_path), annotation_id),
            )

            created += 1

            logging.info(
                "Ellipse mask created: annotation_id=%s path=%s",
                annotation_id,
                mask_path,
            )

        conn.commit()

    return created