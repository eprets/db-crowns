# app/normalize_masks.py
import logging
from pathlib import Path
from typing import Tuple, Optional

import cv2
import numpy as np

from app.db.connection import get_connection


def read_gray_unicode(path: str) -> Optional[np.ndarray]:
    data = np.fromfile(path, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_GRAYSCALE)
    return img


def save_gray_unicode(path: Path, img: np.ndarray) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)

    ok, buf = cv2.imencode(".png", img)
    if not ok:
        return False

    buf.tofile(str(path))
    return True


def normalize_masks(
    db_path: Path,
    roi_mask_norm_dir: Path,
    out_size: Tuple[int, int] = (256, 256),
    only_missing: bool = True,
) -> int:
    """
    Нормализует ROI-маски до размера out_size
    и записывает путь в crown_levels.roi_mask_norm_path.

    Берём только REAL уровни, потому что SYNTH уровни пока не имеют настоящих масок.
    """
    roi_mask_norm_dir.mkdir(parents=True, exist_ok=True)

    processed = 0

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                cl.tree_id,
                cl.h_level,
                cl.source_obs_id,
                cl.roi_mask_norm_path,
                co.roi_mask_raw_path
            FROM crown_levels cl
            JOIN crown_observations co ON co.obs_id = cl.source_obs_id
            WHERE UPPER(cl.data_type) = 'REAL'
            ORDER BY cl.tree_id, cl.h_level
            """
        )

        rows = cur.fetchall()

        for r in rows:
            tree_id = r["tree_id"]
            h_level = float(r["h_level"])

            old_norm = r["roi_mask_norm_path"]
            raw_mask_path = r["roi_mask_raw_path"]

            if old_norm and only_missing:
                logging.info("Skip normalized mask exists: tree=%s h=%.1f", tree_id, h_level)
                continue

            if not raw_mask_path:
                logging.warning("No roi_mask_raw_path: tree=%s h=%.1f", tree_id, h_level)
                continue

            mask = read_gray_unicode(raw_mask_path)

            if mask is None:
                logging.warning("Cannot read roi mask: %s", raw_mask_path)
                continue

            # Для маски используем INTER_NEAREST, чтобы не получить серые значения
            norm = cv2.resize(mask, out_size, interpolation=cv2.INTER_NEAREST)

            # На всякий случай бинаризуем обратно в 0/255
            _, norm_bin = cv2.threshold(norm, 127, 255, cv2.THRESH_BINARY)

            out_path = roi_mask_norm_dir / f"{tree_id}_{h_level:g}.png"

            ok = save_gray_unicode(out_path, norm_bin)

            if not ok:
                logging.warning("Cannot save normalized mask: %s", out_path)
                continue

            cur.execute(
                """
                UPDATE crown_levels
                SET roi_mask_norm_path = ?
                WHERE tree_id = ?
                  AND h_level = ?
                """,
                (str(out_path), tree_id, h_level),
            )

            processed += 1

            logging.info(
                "Normalized mask: tree=%s h=%.1f -> %s",
                tree_id,
                h_level,
                out_path,
            )

        conn.commit()

    return processed