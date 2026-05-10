# app/export_tree_profile.py
import csv
import shutil
from pathlib import Path
from typing import List, Dict, Any

from app.db.connection import get_connection


def _safe_float_name(value: float) -> str:
    """
    Делает красивое имя высоты:
    10.0 -> 10
    12.5 -> 12_5
    """
    if float(value).is_integer():
        return str(int(value))
    return str(value).replace(".", "_")


def _make_output_filename(row: Dict[str, Any]) -> str:
    """
    Формирует имя файла для экспорта уровня.

    Примеры:
    10_REAL.png
    20_SYNTH_linear_blend.png
    55_SYNTH_pix2pix_from_50.png
    """
    h = _safe_float_name(float(row["h_level"]))
    data_type = str(row["data_type"] or "EMPTY").upper()

    synth_method = row.get("synth_method")
    synth_src_h = row.get("synth_src_h")

    if data_type == "REAL":
        return f"{h}_REAL.png"

    if data_type == "SYNTH":
        method = synth_method if synth_method else "unknown"

        if synth_src_h is not None:
            src = _safe_float_name(float(synth_src_h))
            return f"{h}_SYNTH_{method}_from_{src}.png"

        return f"{h}_SYNTH_{method}.png"

    return f"{h}_EMPTY.png"


def export_tree_profile(
    db_path: Path,
    tree_id: str,
    levels_grid: List[float],
    out_root: Path,
) -> Path:
    """
    Экспортирует полный высотный профиль дерева.

    Что делает:
    1. Читает crown_levels для tree_id.
    2. Создаёт папку data/tree_profiles/<tree_id>.
    3. Копирует все найденные roi_norm_path в эту папку.
    4. Создаёт profile.csv с описанием уровней.
    """

    out_dir = out_root / tree_id
    out_dir.mkdir(parents=True, exist_ok=True)

    rows_for_csv = []

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        for h in levels_grid:
            h = float(h)

            cur.execute(
                """
                SELECT
                    tree_id,
                    h_level,
                    data_type,
                    roi_norm_path,
                    mapping_error,
                    synth_method,
                    synth_src_h
                FROM crown_levels
                WHERE tree_id = ?
                  AND h_level = ?
                LIMIT 1
                """,
                (tree_id, h),
            )

            r = cur.fetchone()

            if r is None:
                rows_for_csv.append({
                    "tree_id": tree_id,
                    "h_level": h,
                    "data_type": "EMPTY",
                    "roi_norm_path": "",
                    "exported_file": "",
                    "mapping_error": "",
                    "synth_method": "",
                    "synth_src_h": "",
                    "status": "missing_in_db",
                })
                continue

            row = dict(r)

            src_path_str = row.get("roi_norm_path")
            exported_file = ""
            status = "ok"

            if src_path_str:
                src_path = Path(src_path_str)

                if src_path.exists():
                    out_name = _make_output_filename(row)
                    dst_path = out_dir / out_name
                    shutil.copy2(src_path, dst_path)
                    exported_file = out_name
                else:
                    status = "file_not_found"
            else:
                status = "empty_no_roi"

            rows_for_csv.append({
                "tree_id": tree_id,
                "h_level": h,
                "data_type": row.get("data_type") or "EMPTY",
                "roi_norm_path": src_path_str or "",
                "exported_file": exported_file,
                "mapping_error": row.get("mapping_error") if row.get("mapping_error") is not None else "",
                "synth_method": row.get("synth_method") or "",
                "synth_src_h": row.get("synth_src_h") if row.get("synth_src_h") is not None else "",
                "status": status,
            })

    csv_path = out_dir / "profile.csv"

    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "tree_id",
                "h_level",
                "data_type",
                "roi_norm_path",
                "exported_file",
                "mapping_error",
                "synth_method",
                "synth_src_h",
                "status",
            ],
            delimiter=";"
        )

        writer.writeheader()
        writer.writerows(rows_for_csv)

    return out_dir