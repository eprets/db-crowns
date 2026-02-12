import logging
import uuid
from pathlib import Path
from typing import List, Dict, Tuple, Optional

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


def _blend(img_a: np.ndarray, img_b: np.ndarray, alpha: float) -> np.ndarray:
    """
    alpha=0 -> img_a, alpha=1 -> img_b
    """
    alpha = float(alpha)
    alpha = max(0.0, min(1.0, alpha))
    return cv2.addWeighted(img_a, 1.0 - alpha, img_b, alpha, 0.0)


def _bracketing_levels(real_levels_sorted: List[float], target: float) -> Optional[Tuple[float, float]]:
    """
    Ищем два реальных уровня (low < target < high), между которыми лежит target.
    Если нет таких — возвращаем None.
    """
    low = None
    high = None
    for lv in real_levels_sorted:
        if lv < target:
            low = lv
        if lv > target:
            high = lv
            break
    if low is None or high is None:
        return None
    return (low, high)


def _nearest_level(real_levels: List[float], target: float) -> float:
    """
    Ближайший реальный уровень к target.
    При равенстве расстояний — берём меньший.
    """
    best = real_levels[0]
    best_d = abs(target - best)
    for lv in real_levels[1:]:
        d = abs(target - lv)
        if d < best_d:
            best_d = d
            best = lv
        elif d == best_d and lv < best:
            best = lv
    return best


def synthesize_missing_levels(
    db_path: Path,
    levels_grid: List[float],
    roi_norm_dir: Path,
    only_tree_id: str | None = None,
    fill_only_levels: List[float] | None = None,
    overwrite_existing_synth: bool = False,
) -> int:
    """
    Baseline синтез для пустых уровней crown_levels.

    Метод:
    - Если target между двумя REAL уровнями -> linear_blend
    - Иначе -> nearest_copy (копия ближайшего REAL)

    Пишем:
      crown_levels.data_type='synth'
      crown_levels.synth_method='linear_blend' / 'nearest_copy'
      crown_levels.roi_norm_path -> data/roi_norm/tree_001_20_synth.png

    only_tree_id: синтез только для одного дерева (удобно).
    fill_only_levels: синтез только указанных уровней (например [20.0]).
    overwrite_existing_synth: перезаписать уже существующие synth.
    """
    levels_grid_sorted = sorted([float(x) for x in levels_grid])
    roi_norm_dir.mkdir(parents=True, exist_ok=True)

    created = 0

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        # список деревьев
        if only_tree_id:
            tree_ids = [only_tree_id]
        else:
            cur.execute("SELECT DISTINCT tree_id FROM crown_levels")
            tree_ids = [r["tree_id"] for r in cur.fetchall()]

        if not tree_ids:
            logging.warning("No trees in crown_levels. Nothing to synthesize.")
            return 0

        for tree_id in tree_ids:
            # все уровни дерева
            cur.execute(
                """
                SELECT level_id, h_level, data_type, roi_norm_path, synth_method
                FROM crown_levels
                WHERE tree_id = ?
                """,
                (tree_id,),
            )
            rows = [dict(r) for r in cur.fetchall()]
            by_level = {float(r["h_level"]): r for r in rows}
            existing_levels = set(by_level.keys())

            # реальные уровни (только те, у которых есть roi_norm_path)
            real_levels = [
                lv for lv, r in by_level.items()
                if (r.get("data_type") == "real") and r.get("roi_norm_path")
            ]
            real_levels_sorted = sorted(real_levels)

            if not real_levels_sorted:
                logging.warning("Tree %s: no REAL roi_norm levels. Skip.", tree_id)
                continue

            targets = levels_grid_sorted
            if fill_only_levels is not None:
                targets = sorted([float(x) for x in fill_only_levels])

            for target in targets:
                # если уже есть уровень
                if target in existing_levels:
                    # разрешаем перезапись только если это synth и overwrite=True
                    if by_level[target].get("data_type") == "synth" and overwrite_existing_synth:
                        pass
                    else:
                        continue

                # 1) linear_blend, если target между двумя real
                bracket = _bracketing_levels(real_levels_sorted, target)
                if bracket is not None:
                    low, high = bracket
                    low_path = by_level[low]["roi_norm_path"]
                    high_path = by_level[high]["roi_norm_path"]

                    img_low = read_image_unicode(low_path)
                    img_high = read_image_unicode(high_path)

                    if img_low is None or img_high is None:
                        logging.warning("Cannot read roi_norm for tree=%s low/high=%s/%s", tree_id, low, high)
                        continue

                    alpha = (target - low) / (high - low)
                    synth_img = _blend(img_low, img_high, alpha=alpha)
                    method = "linear_blend"
                else:
                    # 2) nearest_copy
                    nearest = _nearest_level(real_levels_sorted, target)
                    near_path = by_level[nearest]["roi_norm_path"]
                    img_near = read_image_unicode(near_path)
                    if img_near is None:
                        logging.warning("Cannot read nearest roi_norm for tree=%s nearest=%s", tree_id, nearest)
                        continue
                    synth_img = img_near.copy()
                    method = "nearest_copy"

                # имя файла
                level_tag = int(target) if float(target).is_integer() else target
                out_path = roi_norm_dir / f"{tree_id}_{level_tag}_synth.png"
                save_image_unicode(out_path, synth_img)

                # INSERT/UPDATE crown_levels
                cur.execute(
                    """
                    SELECT level_id
                    FROM crown_levels
                    WHERE tree_id = ? AND h_level = ?
                    LIMIT 1
                    """,
                    (tree_id, target),
                )
                ex = cur.fetchone()

                if ex is None:
                    level_id = str(uuid.uuid4())
                    cur.execute(
                        """
                        INSERT INTO crown_levels
                        (level_id, tree_id, h_level, source_obs_id, data_type, mapping_error,
                         roi_norm_path, synth_method)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            level_id,
                            tree_id,
                            float(target),
                            None,
                            "synth",
                            None,
                            str(out_path),
                            method,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE crown_levels
                        SET data_type = 'synth',
                            roi_norm_path = ?,
                            synth_method = ?,
                            created_at = CURRENT_TIMESTAMP
                        WHERE level_id = ?
                        """,
                        (str(out_path), method, ex["level_id"]),
                    )

                created += 1
                logging.info("Synth created: tree=%s h=%s method=%s -> %s", tree_id, target, method, str(out_path))

        conn.commit()

    logging.info("synthesize_missing_levels done. created=%d", created)
    return created
