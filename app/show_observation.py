from pathlib import Path
import cv2
import numpy as np

from app.db.connection import get_connection


def read_image_unicode(path: str):
    data = np.fromfile(path, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    return img


def show_observation(db_path: Path, obs_id: str) -> None:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT roi_raw_path
            FROM crown_observations
            WHERE obs_id = ?
            """,
            (obs_id,),
        )
        row = cur.fetchone()

    if row is None:
        raise RuntimeError(f"Observation not found: {obs_id}")

    roi_path = row["roi_raw_path"]
    img = read_image_unicode(roi_path)
    if img is None:
        raise RuntimeError(f"Cannot read ROI image: {roi_path}")

    win = f"ROI | obs_id={obs_id}"
    cv2.namedWindow(win, cv2.WINDOW_NORMAL)
    cv2.imshow(win, img)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
