import json
import logging
import math
import uuid
from pathlib import Path
from typing import Dict, Any, List

import cv2
import numpy as np

from app.db.connection import get_connection


def read_image_unicode(path: str):
    data = np.fromfile(path, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    return img


def crop_roi(img, x0: float, y0: float, a: float, b: float, padding_px: int):
    """
    Вырезаем ROI по bounding box эллипса + padding.
    """
    h, w = img.shape[:2]

    xmin = int(max(0, math.floor(x0 - a - padding_px)))
    xmax = int(min(w, math.ceil(x0 + a + padding_px)))
    ymin = int(max(0, math.floor(y0 - b - padding_px)))
    ymax = int(min(h, math.ceil(y0 + b + padding_px)))

    roi = img[ymin:ymax, xmin:xmax].copy()
    return roi, (xmin, ymin, xmax, ymax)


def compute_simple_features(roi, a: float, b: float) -> Dict[str, Any]:
    """
    Базовые признаки:
    - площадь эллипса
    - отношение осей
    - яркость/контраст ROI
    """
    area_ellipse = float(math.pi * a * b)
    axis_ratio = float(a / b) if b != 0 else None

    gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
    mean = float(np.mean(gray))
    std = float(np.std(gray))

    return {
        "ellipse_area": area_ellipse,
        "axis_ratio": axis_ratio,
        "roi_mean_gray": mean,
        "roi_std_gray": std,
    }


def build_observations(
    db_path: Path,
    roi_raw_dir: Path,
    padding_px: int = 20,
    limit: int | None = None
) -> int:
    """
    Создаёт ROI и записи crown_observations для всех аннотаций.
    Возвращает число добавленных наблюдений.
    """
    roi_raw_dir.mkdir(parents=True, exist_ok=True)
    added = 0

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        q = """
        SELECT
            a.annotation_id, a.image_id, a.tree_id,
            a.x0, a.y0, a.a, a.b, a.theta,
            i.path AS image_path
        FROM annotations a
        JOIN images i ON i.image_id = a.image_id
        ORDER BY a.created_at ASC
        """
        if limit is not None:
            q += " LIMIT ?"
            cur.execute(q, (limit,))
        else:
            cur.execute(q)

        rows: List[Dict[str, Any]] = [dict(r) for r in cur.fetchall()]

        for r in rows:
            annotation_id = r["annotation_id"]

            # если уже есть observation для этой аннотации — пропускаем
            cur.execute(
                "SELECT 1 FROM crown_observations WHERE annotation_id = ? LIMIT 1",
                (annotation_id,)
            )
            if cur.fetchone() is not None:
                logging.info("Skip: observation already exists for annotation %s", annotation_id)
                continue

            img = read_image_unicode(r["image_path"])
            if img is None:
                logging.warning("Cannot read image: %s", r["image_path"])
                continue

            roi, bbox = crop_roi(
                img,
                x0=float(r["x0"]),
                y0=float(r["y0"]),
                a=float(r["a"]),
                b=float(r["b"]),
                padding_px=padding_px,
            )

            obs_id = str(uuid.uuid4())
            roi_path = roi_raw_dir / f"{obs_id}.png"

            # сохраняем ROI (unicode-safe)
            ok, buf = cv2.imencode(".png", roi)
            if not ok:
                logging.warning("Cannot encode ROI for annotation %s", annotation_id)
                continue
            buf.tofile(str(roi_path))

            features = compute_simple_features(roi, float(r["a"]), float(r["b"]))
            features["bbox"] = {"xmin": bbox[0], "ymin": bbox[1], "xmax": bbox[2], "ymax": bbox[3]}

            cur.execute(
                """
                INSERT INTO crown_observations
                (obs_id, annotation_id, image_id, tree_id, roi_raw_path, obs_height, features_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    obs_id,
                    annotation_id,
                    r["image_id"],
                    r["tree_id"],
                    str(roi_path),
                    None,
                    json.dumps(features, ensure_ascii=False),
                ),
            )
            added += 1
            logging.info("Built observation %s for annotation %s", obs_id, annotation_id)

        conn.commit()

    return added
