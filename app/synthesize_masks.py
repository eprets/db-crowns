# app/synthesize_masks.py

import logging
import shutil
from pathlib import Path

from app.db.connection import get_connection


def synthesize_masks_for_synth_levels(
    db_path: Path,
    roi_mask_norm_dir: Path,
    overwrite: bool = False,
) -> int:
    """
    Создаёт маски для SYNTH-уровней.

    Логика:
    - ищем SYNTH уровень;
    - смотрим synth_src_h;
    - берём маску уровня-источника;
    - копируем её как маску синтезированного уровня;
    - записываем путь в crown_levels.roi_mask_norm_path.

    Это baseline-подход:
    mask(target_h) = mask(source_h)
    """

    roi_mask_norm_dir.mkdir(parents=True, exist_ok=True)

    created = 0

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                tree_id,
                h_level,
                data_type,
                synth_method,
                synth_src_h,
                roi_mask_norm_path
            FROM crown_levels
            WHERE UPPER(data_type) = 'SYNTH'
            ORDER BY tree_id, h_level
            """
        )

        synth_rows = cur.fetchall()

        for r in synth_rows:
            tree_id = r["tree_id"]
            target_h = float(r["h_level"])
            synth_src_h = r["synth_src_h"]
            old_mask = r["roi_mask_norm_path"]

            if old_mask and not overwrite:
                logging.info(
                    "Skip synth mask exists: tree=%s h=%.1f",
                    tree_id,
                    target_h,
                )
                continue

            if synth_src_h is None:
                logging.warning(
                    "No synth_src_h for SYNTH level: tree=%s h=%.1f",
                    tree_id,
                    target_h,
                )
                continue

            src_h = float(synth_src_h)

            # Ищем маску источника
            cur.execute(
                """
                SELECT roi_mask_norm_path
                FROM crown_levels
                WHERE tree_id = ?
                  AND h_level = ?
                LIMIT 1
                """,
                (tree_id, src_h),
            )

            src = cur.fetchone()

            if src is None or not src["roi_mask_norm_path"]:
                logging.warning(
                    "Source mask not found: tree=%s src_h=%.1f target_h=%.1f",
                    tree_id,
                    src_h,
                    target_h,
                )
                continue

            src_mask_path = Path(src["roi_mask_norm_path"])

            if not src_mask_path.exists():
                logging.warning(
                    "Source mask file does not exist: %s",
                    src_mask_path,
                )
                continue

            out_path = roi_mask_norm_dir / f"{tree_id}_{target_h:g}_synth_mask_from_{src_h:g}.png"

            shutil.copy2(src_mask_path, out_path)

            cur.execute(
                """
                UPDATE crown_levels
                SET roi_mask_norm_path = ?
                WHERE tree_id = ?
                  AND h_level = ?
                """,
                (
                    str(out_path),
                    tree_id,
                    target_h,
                ),
            )

            created += 1

            logging.info(
                "Synth mask created: tree=%s h=%.1f from %.1f -> %s",
                tree_id,
                target_h,
                src_h,
                out_path,
            )

        conn.commit()

    return created