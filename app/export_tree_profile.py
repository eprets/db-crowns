# app/export_tree_profile.py
import csv
import shutil
from pathlib import Path
from typing import List, Dict, Any

from app.db.connection import get_connection


def _safe_float_name(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return str(value).replace(".", "_")


def _make_image_filename(row: Dict[str, Any]) -> str:
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


def _make_mask_filename(row: Dict[str, Any]) -> str:
    h = _safe_float_name(float(row["h_level"]))
    data_type = str(row["data_type"] or "EMPTY").upper()

    if data_type == "REAL":
        return f"{h}_REAL_mask.png"

    if data_type == "SYNTH":
        method = row.get("synth_method") or "unknown"
        synth_src_h = row.get("synth_src_h")

        if synth_src_h is not None:
            src = _safe_float_name(float(synth_src_h))
            return f"{h}_SYNTH_{method}_from_{src}_mask.png"

        return f"{h}_SYNTH_{method}_mask.png"

    return f"{h}_EMPTY_mask.png"


def export_tree_profile(
    db_path: Path,
    tree_id: str,
    levels_grid: List[float],
    out_root: Path,
) -> Path:
    """
    Экспортирует высотный профиль дерева.

    Создаёт:
    data/tree_profiles/<tree_id>/
      profile.csv
      images/
      masks/

    В images копируются roi_norm_path.
    В masks копируются roi_mask_norm_path, если маска есть.
    """

    out_dir = out_root / tree_id
    images_dir = out_dir / "images"
    masks_dir = out_dir / "masks"

    images_dir.mkdir(parents=True, exist_ok=True)
    masks_dir.mkdir(parents=True, exist_ok=True)

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
                    roi_mask_norm_path,
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
                    "roi_mask_norm_path": "",
                    "image_file": "",
                    "mask_file": "",
                    "mapping_error": "",
                    "synth_method": "",
                    "synth_src_h": "",
                    "image_status": "missing_in_db",
                    "mask_status": "missing_in_db",
                })
                continue

            row = dict(r)

            # ---------- image export ----------
            image_file = ""
            image_status = "ok"

            src_img_str = row.get("roi_norm_path")

            if src_img_str:
                src_img = Path(src_img_str)

                if src_img.exists():
                    img_name = _make_image_filename(row)
                    dst_img = images_dir / img_name
                    shutil.copy2(src_img, dst_img)
                    image_file = str(Path("images") / img_name)
                else:
                    image_status = "file_not_found"
            else:
                image_status = "empty_no_roi"

            # ---------- mask export ----------
            mask_file = ""
            mask_status = "ok"

            src_mask_str = row.get("roi_mask_norm_path")

            if src_mask_str:
                src_mask = Path(src_mask_str)

                if src_mask.exists():
                    mask_name = _make_mask_filename(row)
                    dst_mask = masks_dir / mask_name
                    shutil.copy2(src_mask, dst_mask)
                    mask_file = str(Path("masks") / mask_name)
                else:
                    mask_status = "mask_file_not_found"
            else:
                mask_status = "no_mask"

            rows_for_csv.append({
                "tree_id": tree_id,
                "h_level": h,
                "data_type": row.get("data_type") or "EMPTY",
                "roi_norm_path": src_img_str or "",
                "roi_mask_norm_path": src_mask_str or "",
                "image_file": image_file,
                "mask_file": mask_file,
                "mapping_error": row.get("mapping_error") if row.get("mapping_error") is not None else "",
                "synth_method": row.get("synth_method") or "",
                "synth_src_h": row.get("synth_src_h") if row.get("synth_src_h") is not None else "",
                "image_status": image_status,
                "mask_status": mask_status,
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
                "roi_mask_norm_path",
                "image_file",
                "mask_file",
                "mapping_error",
                "synth_method",
                "synth_src_h",
                "image_status",
                "mask_status",
            ],
            delimiter=";",
        )

        writer.writeheader()
        writer.writerows(rows_for_csv)

    return out_dir