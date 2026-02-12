import logging
import math
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Optional

from app.db.connection import get_connection


@dataclass
class ObsRow:
    obs_id: str
    tree_id: str
    obs_height: float


def _nearest_level(obs_h: float, levels: List[float]) -> float:
    """
    Выбирает ближайший уровень сетки.
    Если одинаково близко к двум уровням — берём меньший (стабильное правило).
    """
    best_level = levels[0]
    best_dist = abs(obs_h - best_level)

    for lv in levels[1:]:
        d = abs(obs_h - lv)
        if d < best_dist:
            best_dist = d
            best_level = lv
        elif d == best_dist and lv < best_level:
            best_level = lv

    return float(best_level)


def build_levels(
    db_path: Path,
    levels: List[float],
    data_type_real: str = "real",
) -> int:
    """
    Создаёт/обновляет записи crown_levels для всех деревьев на заданной сетке высот.

    Логика:
    - Берём observations с obs_height NOT NULL
    - Для каждого obs вычисляем ближайший h_level
    - Если на один (tree_id, h_level) попало несколько obs — выбираем тот, у которого mapping_error меньше
    - Записываем crown_levels (data_type='real')
    Возвращает количество вставленных/обновлённых строк.
    """
    if not levels:
        raise ValueError("levels list is empty")

    levels_sorted = sorted([float(x) for x in levels])

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        # 1) Берём все observations с высотой
        cur.execute(
            """
            SELECT obs_id, tree_id, obs_height
            FROM crown_observations
            WHERE obs_height IS NOT NULL
            """
        )
        obs_rows = cur.fetchall()
        if not obs_rows:
            logging.warning("No observations with obs_height. Cannot build levels.")
            return 0

        # 2) Лучший кандидат на каждый (tree_id, h_level)
        # key -> (best_obs_id, best_err)
        best: Dict[tuple, tuple] = {}

        for r in obs_rows:
            obs_id = r["obs_id"]
            tree_id = r["tree_id"]
            obs_h = float(r["obs_height"])

            h_level = _nearest_level(obs_h, levels_sorted)
            err = abs(obs_h - h_level)

            key = (tree_id, h_level)
            if key not in best:
                best[key] = (obs_id, err)
            else:
                prev_obs_id, prev_err = best[key]
                if err < prev_err:
                    best[key] = (obs_id, err)

        # 3) Записываем в crown_levels:
        # если запись уже есть -> UPDATE (только если data_type='real', чтобы не затирать synth позже)
        changed = 0

        for (tree_id, h_level), (obs_id, err) in best.items():
            # проверяем, есть ли уже строка
            cur.execute(
                """
                SELECT level_id, data_type
                FROM crown_levels
                WHERE tree_id = ? AND h_level = ?
                LIMIT 1
                """,
                (tree_id, h_level),
            )
            row = cur.fetchone()

            if row is None:
                level_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO crown_levels
                    (level_id, tree_id, h_level, source_obs_id, data_type, mapping_error)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (level_id, tree_id, h_level, obs_id, data_type_real, float(err)),
                )
                changed += 1
            else:
                # если там synth — пока не трогаем, чтобы потом не ломать синтез
                if row["data_type"] != data_type_real:
                    continue

                cur.execute(
                    """
                    UPDATE crown_levels
                    SET source_obs_id = ?,
                        mapping_error = ?,
                        created_at = CURRENT_TIMESTAMP
                    WHERE level_id = ?
                    """,
                    (obs_id, float(err), row["level_id"]),
                )
                changed += 1

        conn.commit()

    logging.info("build_levels done. Upserted %d rows.", changed)
    return changed


def show_levels(db_path: Path, tree_id: str, levels: List[float]) -> None:
    """
    Печатает профиль дерева по уровням сетки: real/synth/empty.
    """
    levels_sorted = sorted([float(x) for x in levels])

    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT h_level, data_type, source_obs_id, mapping_error, roi_norm_path
            FROM crown_levels
            WHERE tree_id = ?
            """,
            (tree_id,),
        )
        rows = cur.fetchall()

    by_level = {float(r["h_level"]): dict(r) for r in rows}

    print(f"\n=== LEVELS for tree_id={tree_id} ===")
    for lv in levels_sorted:
        if lv not in by_level:
            print(f"- {lv:>5} m : EMPTY")
        else:
            r = by_level[lv]
            dt = r.get("data_type")
            err = r.get("mapping_error")
            roi_norm = r.get("roi_norm_path")
            print(f"- {lv:>5} m : {dt.upper():<5}  err={err}  roi_norm={roi_norm}")
