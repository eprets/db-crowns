import logging
import math
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import cv2
import numpy as np

from app.db.connection import get_connection
from app.observations_manager import rebuild_observation_for_annotation


@dataclass
class EllipseParams:
    x0: float
    y0: float
    a: float
    b: float
    theta: float  # radians


class EllipseAnnotator:
    """
    Простейший аннотатор эллипса.

    Управление:
    - ЛКМ: поставить/обновить центр эллипса
    - ПКМ: задать радиусы (a,b) по расстоянию от центра (простая версия)
    - Колёсико мыши: вращение theta
    - S: сохранить аннотацию
    - N / P: следующее / предыдущее изображение
    - R: сброс эллипса
    - Q или Esc: выход
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.image_rows: List[Dict] = []
        self.idx: int = 0

        self.img = None
        self.img_disp = None

        self.center: Optional[Tuple[int, int]] = None
        self.a: Optional[float] = None
        self.b: Optional[float] = None
        self.theta: float = 0.0  # radians

        self.tree_id: str = ""
        self.tree_type: str = ""

    def load_images_from_db(self) -> None:
        with get_connection(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT image_id, path FROM images ORDER BY created_at ASC")
            self.image_rows = [dict(r) for r in cur.fetchall()]

        if not self.image_rows:
            raise RuntimeError("No images in DB. Run: python -m app.main import")

    def _read_image_unicode_path(self, path: str):

        data = np.fromfile(path, dtype=np.uint8)
        img = cv2.imdecode(data, cv2.IMREAD_COLOR)
        return img

    def _load_current_image(self) -> None:
        row = self.image_rows[self.idx]
        path = row["path"]

        img = self._read_image_unicode_path(path)
        if img is None:
            raise RuntimeError(f"Cannot read image: {path}")

        self.img = img
        self.img_disp = self.img.copy()

        # сброс текущего эллипса при переключении картинки
        self.center = None
        self.a = None
        self.b = None
        self.theta = 0.0

        logging.info("Opened image %d/%d: %s", self.idx + 1, len(self.image_rows), path)

    def _draw_overlay(self) -> None:
        self.img_disp = self.img.copy()

        row = self.image_rows[self.idx]
        text1 = f"{self.idx + 1}/{len(self.image_rows)}  image_id={row['image_id']}"
        text2 = f"tree_id={self.tree_id}  tree_type={self.tree_type}"
        text3 = "LMB:center  RMB:radii  Wheel:rotate  S:save  N/P:nav  R:reset  Q/Esc:quit"

        cv2.putText(self.img_disp, text1, (10, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (30, 255, 30), 2)
        cv2.putText(self.img_disp, text2, (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (30, 255, 30), 2)
        cv2.putText(self.img_disp, text3, (10, 75), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (30, 255, 30), 2)

        if self.center is not None:
            cv2.circle(self.img_disp, self.center, 4, (0, 255, 255), -1)

        if self.center is not None and self.a is not None and self.b is not None:
            center = (int(self.center[0]), int(self.center[1]))
            axes = (int(self.a), int(self.b))
            angle_deg = float(self.theta * 180.0 / math.pi)
            cv2.ellipse(self.img_disp, center, axes, angle_deg, 0, 360, (0, 0, 255), 2)

    def _mouse_callback(self, event, x, y, flags, param) -> None:
        # ЛКМ — центр
        if event == cv2.EVENT_LBUTTONDOWN:
            self.center = (x, y)
            logging.info("Center set: %s", self.center)

        # ПКМ — радиусы по расстоянию от центра (простая схема)
        if event == cv2.EVENT_RBUTTONDOWN:
            if self.center is None:
                logging.warning("Set center first (LMB).")
                return

            dx = x - self.center[0]
            dy = y - self.center[1]
            r = math.sqrt(dx * dx + dy * dy)

            self.a = max(5.0, r)
            self.b = max(5.0, 0.7 * r)  # простое приближение
            logging.info("Radii set: a=%.1f b=%.1f", self.a, self.b)

        # Колесо — вращение
        if event == cv2.EVENT_MOUSEWHEEL:
            step = 5.0 * math.pi / 180.0  # 5 градусов
            if flags > 0:
                self.theta += step
            else:
                self.theta -= step
            logging.info("Theta: %.1f deg", self.theta * 180.0 / math.pi)

    def _get_current_params(self) -> Optional[EllipseParams]:
        if self.center is None or self.a is None or self.b is None:
            return None

        return EllipseParams(
            x0=float(self.center[0]),
            y0=float(self.center[1]),
            a=float(self.a),
            b=float(self.b),
            theta=float(self.theta),
        )

    def save_annotation(self) -> None:
        logging.info("SAVE pressed")

        params = self._get_current_params()
        if params is None:
            logging.warning("Cannot save: ellipse not complete. Set center (LMB) and radii (RMB).")
            return

        if not self.tree_id:
            logging.warning("Cannot save: tree_id is empty.")
            return

        row = self.image_rows[self.idx]
        image_id = row["image_id"]

        annotation_id = str(uuid.uuid4())

        with get_connection(self.db_path) as conn:
            cur = conn.cursor()

            # гарантируем наличие дерева в trees
            cur.execute(
                """
                INSERT OR IGNORE INTO trees (tree_id, tree_type)
                VALUES (?, ?)
                """,
                (self.tree_id, self.tree_type if self.tree_type else None),
            )

            # сохраняем аннотацию
            # Проверяем, есть ли уже аннотация на (image_id, tree_id)
            cur.execute(
                """
                SELECT annotation_id
                FROM annotations
                WHERE image_id = ?
                  AND tree_id = ? LIMIT 1
                """,
                (image_id, self.tree_id),
            )
            existing = cur.fetchone()

            if existing is not None:
                existing_id = existing["annotation_id"]

                cur.execute(
                    """
                    UPDATE annotations
                    SET tree_type  = ?,
                        x0         = ?,
                        y0         = ?,
                        a          = ?,
                        b          = ?,
                        theta      = ?,
                        quality    = ?,
                        created_at = CURRENT_TIMESTAMP
                    WHERE annotation_id = ?
                    """,
                    (
                        self.tree_type if self.tree_type else None,
                        params.x0, params.y0, params.a, params.b, params.theta,
                        None,
                        existing_id,
                    ),
                )
                logging.info(
                    "Updated annotation %s for image_id=%s tree_id=%s",
                    existing_id, image_id, self.tree_id
                )

                final_annotation_id = existing_id

            else:
                annotation_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO annotations
                    (annotation_id, image_id, tree_id, tree_type, x0, y0, a, b, theta, quality)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        annotation_id,
                        image_id,
                        self.tree_id,
                        self.tree_type if self.tree_type else None,
                        params.x0, params.y0, params.a, params.b, params.theta,
                        None,
                    ),
                )
                logging.info(
                    "Inserted annotation %s for image_id=%s tree_id=%s",
                    annotation_id, image_id, self.tree_id
                )

                final_annotation_id = annotation_id

            conn.commit()

            # --- Авто-перестройка observation ---
            roi_raw_dir = Path("data/roi_raw")
            padding_px = 20

            new_obs_id = rebuild_observation_for_annotation(
                db_path=self.db_path,
                annotation_id=final_annotation_id,
                roi_raw_dir=roi_raw_dir,
                padding_px=padding_px,
            )

            logging.info("Auto rebuild observation done. New obs_id=%s", new_obs_id)

    def run(self, tree_id: str, tree_type: str) -> None:
        self.tree_id = tree_id
        self.tree_type = tree_type

        self.load_images_from_db()
        self._load_current_image()

        win = "Ellipse Annotator"
        cv2.namedWindow(win, cv2.WINDOW_NORMAL)
        cv2.setMouseCallback(win, self._mouse_callback)

        while True:
            self._draw_overlay()
            cv2.imshow(win, self.img_disp)

            key = cv2.waitKey(20) & 0xFF

            # выход
            if key in (27, ord("q"), ord("Q")):
                break

            # сохранить
            if key in (ord("s"), ord("S")):
                self.save_annotation()

            # сброс
            if key in (ord("r"), ord("R")):
                self.center = None
                self.a = None
                self.b = None
                self.theta = 0.0
                logging.info("Ellipse reset")

            # след/пред
            if key in (ord("n"), ord("N")):
                self.idx = min(self.idx + 1, len(self.image_rows) - 1)
                self._load_current_image()

            if key in (ord("p"), ord("P")):
                self.idx = max(self.idx - 1, 0)
                self._load_current_image()

        cv2.destroyAllWindows()
