import logging
from pathlib import Path
from typing import Tuple

import cv2
import numpy as np

from app.db.connection import get_connection


def read_image_unicode(path: str):
    data = np.fromfile(path, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    return img


def save_image_unicode(path: Path, img) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ok, buf = cv2.imencode(".png", img)
    if not ok:
        raise RuntimeError(f"Cannot encode image for saving: {path}")
    buf.tofile(str(path))


def pyramid_resize(img, out_size: Tuple[int, int]) -> np.ndarray:
    """
    Простая пирамидальная нормализация:
    - несколько раз pyrDown, если изображение сильно больше нужного
    - затем resize до out_size

    Это не "супер-разрешение", но хороший честный baseline:
    стабилизирует масштаб/размер ROI.
    """
    target_w, target_h = int(out_size[0]), int(out_size[1])
    h, w = img.shape[:2]

    # Если изображение очень большое, плавно уменьшаем пирамидой
    tmp = img
    while tmp.shape[0] > target_h * 2 and tmp.shape[1] > target_w * 2:
        tmp = cv2.pyrDown(tmp)

    # Финальное приведение к размеру
    norm = cv2.resize(tmp, (target_w, target_h), interpolation=cv2.INTER_AREA)
    return norm


def normalize_scale(
    db_path: Path,
    roi_norm_dir: Path,
    out_size: Tuple[int, int] = (256, 256),
    only_missing: bool = True,
) -> int:
    """
    Нормализует ROI для уровней crown_levels (data_type='real'):
    roi_raw -> roi_norm (256x256) и записывает путь в crown_levels.roi_norm_path

    only_missing=True: не пересоздаёт, если roi_norm_path уже заполнен.
    Возвращает количество обработанных уровней.
    """
    roi_norm_dir.mkdir(parents=True, exist_ok=True)
    processed = 0

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        # Берём уровни real
        cur.execute(
            """
            SELECT level_id, tree_id, h_level, source_obs_id, roi_norm_path
            FROM crown_levels
            WHERE data_type = 'real'
            """
        )
        levels = cur.fetchall()

        if not levels:
            logging.warning("No crown_levels rows with data_type='real'. Nothing to normalize.")
            return 0

        for lv in levels:
            level_id = lv["level_id"]
            tree_id = lv["tree_id"]
            h_level = float(lv["h_level"])
            source_obs_id = lv["source_obs_id"]
            existing_norm = lv["roi_norm_path"]

            if only_missing and existing_norm:
                continue

            if not source_obs_id:
                logging.warning("Level %s has no source_obs_id. Skip.", level_id)
                continue

            # достаём roi_raw_path
            cur.execute(
                """
                SELECT roi_raw_path
                FROM crown_observations
                WHERE obs_id = ?
                LIMIT 1
                """,
                (source_obs_id,),
            )
            row = cur.fetchone()
            if row is None:
                logging.warning("Observation not found for obs_id=%s. Skip.", source_obs_id)
                continue

            roi_raw_path = row["roi_raw_path"]
            img = read_image_unicode(roi_raw_path)
            if img is None:
                logging.warning("Cannot read roi_raw image: %s", roi_raw_path)
                continue

            norm = pyramid_resize(img, out_size=out_size)

            # имя файла: tree_001_15.png (15 -> 15, 15.0 -> 15)
            level_int = int(h_level) if float(h_level).is_integer() else h_level
            out_path = roi_norm_dir / f"{tree_id}_{level_int}.png"

            save_image_unicode(out_path, norm)

            # записываем путь в crown_levels
            cur.execute(
                """
                UPDATE crown_levels
                SET roi_norm_path = ?, created_at = CURRENT_TIMESTAMP
                WHERE level_id = ?
                """,
                (str(out_path), level_id),
            )
            processed += 1
            logging.info("Normalized level: tree_id=%s h_level=%s -> %s", tree_id, h_level, str(out_path))

        conn.commit()

    logging.info("normalize_scale done. Processed=%d", processed)
    return processed
