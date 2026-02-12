import json
import logging
import math
import uuid
from pathlib import Path
from typing import Optional, Dict, Any

import cv2
import numpy as np

from app.db.connection import get_connection


def read_image_unicode(path: str):
    data = np.fromfile(path, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    return img


def crop_roi(img, x0: float, y0: float, a: float, b: float, padding_px: int):
    h, w = img.shape[:2]
    xmin = int(max(0, math.floor(x0 - a - padding_px)))
    xmax = int(min(w, math.ceil(x0 + a + padding_px)))
    ymin = int(max(0, math.floor(y0 - b - padding_px)))
    ymax = int(min(h, math.ceil(y0 + b + padding_px)))
    roi = img[ymin:ymax, xmin:xmax].copy()
    return roi, (xmin, ymin, xmax, ymax)


def compute_simple_features(roi, a: float, b: float) -> Dict[str, Any]:
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


def rebuild_observation_for_annotation(
    db_path: Path,
    annotation_id: str,
    roi_raw_dir: Path,
    padding_px: int,
) -> Optional[str]:
    """
    Пересобирает observation для конкретной annotation_id:
    - удаляет старый observation + ROI-файл (если есть)
    - строит новый ROI и признаки
    - записывает в crown_observations
    Возвращает новый obs_id или None (если не удалось).

    ВАЖНО: obs_height берём из images.flight_altitude
    """
    roi_raw_dir.mkdir(parents=True, exist_ok=True)

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        # 1) достаём аннотацию + путь к исходному изображению + flight_altitude
        cur.execute(
            """
            SELECT
                a.annotation_id, a.image_id, a.tree_id,
                a.x0, a.y0, a.a, a.b,
                i.path AS image_path,
                i.flight_altitude AS flight_altitude
            FROM annotations a
            JOIN images i ON i.image_id = a.image_id
            WHERE a.annotation_id = ?
            """,
            (annotation_id,),
        )
        row = cur.fetchone()
        if row is None:
            logging.warning("Annotation not found: %s", annotation_id)
            return None

        image_path = row["image_path"]
        obs_height = row["flight_altitude"]

        # 2) если есть старый observation — удалить его и его файл ROI
        cur.execute(
            "SELECT obs_id, roi_raw_path FROM crown_observations WHERE annotation_id = ? LIMIT 1",
            (annotation_id,),
        )
        old = cur.fetchone()
        if old is not None:
            old_roi = old["roi_raw_path"]
            try:
                Path(old_roi).unlink(missing_ok=True)
            except Exception:
                pass

            cur.execute("DELETE FROM crown_observations WHERE annotation_id = ?", (annotation_id,))
            logging.info("Old observation removed for annotation %s", annotation_id)

        # 3) строим новый ROI
        img = read_image_unicode(image_path)
        if img is None:
            logging.warning("Cannot read image: %s", image_path)
            return None

        roi, bbox = crop_roi(
            img,
            x0=float(row["x0"]),
            y0=float(row["y0"]),
            a=float(row["a"]),
            b=float(row["b"]),
            padding_px=padding_px,
        )

        obs_id = str(uuid.uuid4())
        roi_path = roi_raw_dir / f"{obs_id}.png"

        ok, buf = cv2.imencode(".png", roi)
        if not ok:
            logging.warning("Cannot encode ROI for annotation %s", annotation_id)
            return None
        buf.tofile(str(roi_path))

        features = compute_simple_features(roi, float(row["a"]), float(row["b"]))
        features["bbox"] = {"xmin": bbox[0], "ymin": bbox[1], "xmax": bbox[2], "ymax": bbox[3]}

        # 4) записываем новый observation
        cur.execute(
            """
            INSERT INTO crown_observations
            (obs_id, annotation_id, image_id, tree_id, roi_raw_path, obs_height, features_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                obs_id,
                annotation_id,
                row["image_id"],
                row["tree_id"],
                str(roi_path),
                obs_height,
                json.dumps(features, ensure_ascii=False),
            ),
        )
        conn.commit()

        logging.info(
            "Observation rebuilt: obs_id=%s for annotation_id=%s (obs_height=%s)",
            obs_id, annotation_id, str(obs_height)
        )
        return obs_id
