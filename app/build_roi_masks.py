# app/build_roi_masks.py
import json
import logging
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

from app.db.connection import get_connection


def read_gray_unicode(path: str) -> Optional[np.ndarray]:
    """
    Безопасно читает grayscale-изображение по пути с русскими буквами.
    """
    data = np.fromfile(path, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_GRAYSCALE)
    return img


def save_gray_unicode(path: Path, img: np.ndarray) -> bool:
    """
    Безопасно сохраняет grayscale-изображение PNG по пути с русскими буквами.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    ok, buf = cv2.imencode(".png", img)
    if not ok:
        return False

    buf.tofile(str(path))
    return True


def build_roi_masks(
    db_path: Path,
    roi_mask_raw_dir: Path,
    overwrite: bool = False,
) -> int:
    """
    Создаёт ROI-маски для crown_observations.

    Логика:
    - берём observation;
    - находим annotation.mask_path;
    - из features_json берём bbox;
    - вырезаем из полной маски этот bbox;
    - сохраняем data/roi_mask_raw/<obs_id>.png;
    - пишем путь в crown_observations.roi_mask_raw_path.
    """

    roi_mask_raw_dir.mkdir(parents=True, exist_ok=True)

    created = 0

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                o.obs_id,
                o.annotation_id,
                o.features_json,
                o.roi_mask_raw_path,
                a.mask_path
            FROM crown_observations o
            JOIN annotations a ON a.annotation_id = o.annotation_id
            ORDER BY o.created_at ASC
            """
        )

        rows = cur.fetchall()

        for r in rows:
            obs_id = r["obs_id"]
            old_roi_mask = r["roi_mask_raw_path"]
            mask_path = r["mask_path"]

            if old_roi_mask and not overwrite:
                logging.info("Skip ROI mask exists: obs_id=%s", obs_id)
                continue

            if not mask_path:
                logging.warning("No annotation mask_path for obs_id=%s", obs_id)
                continue

            full_mask = read_gray_unicode(mask_path)

            if full_mask is None:
                logging.warning("Cannot read mask: %s", mask_path)
                continue

            features_json = r["features_json"]

            if not features_json:
                logging.warning("No features_json for obs_id=%s", obs_id)
                continue

            try:
                features = json.loads(features_json)
                bbox = features["bbox"]
                xmin = int(bbox["xmin"])
                ymin = int(bbox["ymin"])
                xmax = int(bbox["xmax"])
                ymax = int(bbox["ymax"])
            except Exception as e:
                logging.warning("Cannot parse bbox for obs_id=%s: %s", obs_id, e)
                continue

            h, w = full_mask.shape[:2]

            xmin = max(0, min(xmin, w))
            xmax = max(0, min(xmax, w))
            ymin = max(0, min(ymin, h))
            ymax = max(0, min(ymax, h))

            if xmax <= xmin or ymax <= ymin:
                logging.warning("Invalid bbox for obs_id=%s", obs_id)
                continue

            roi_mask = full_mask[ymin:ymax, xmin:xmax].copy()

            out_path = roi_mask_raw_dir / f"{obs_id}.png"

            ok = save_gray_unicode(out_path, roi_mask)

            if not ok:
                logging.warning("Cannot save ROI mask: %s", out_path)
                continue

            cur.execute(
                """
                UPDATE crown_observations
                SET roi_mask_raw_path = ?
                WHERE obs_id = ?
                """,
                (str(out_path), obs_id),
            )

            created += 1

            logging.info("ROI mask created: obs_id=%s path=%s", obs_id, out_path)

        conn.commit()

    return created