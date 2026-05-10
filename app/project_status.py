# app/project_status.py

from pathlib import Path
from typing import List

from app.db.connection import get_connection


def print_project_status(
    db_path: Path,
    tree_id: str,
    levels_grid: List[float],
) -> None:
    """
    Печатает сводку по дереву:
    - уровни;
    - REAL/SYNTH/EMPTY;
    - наличие ROI;
    - наличие масок;
    - экспортированные файлы.
    """

    total = len(levels_grid)
    real_count = 0
    synth_count = 0
    empty_count = 0

    roi_count = 0
    mask_count = 0

    missing_roi = []
    missing_mask = []

    rows_print = []

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        for h in levels_grid:
            h = float(h)

            cur.execute(
                """
                SELECT
                    h_level,
                    data_type,
                    roi_norm_path,
                    roi_mask_norm_path,
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
                empty_count += 1
                missing_roi.append(h)
                missing_mask.append(h)

                rows_print.append(
                    f"- {h:6.1f} m | EMPTY | roi=NO  | mask=NO"
                )
                continue

            data_type = str(r["data_type"] or "EMPTY").upper()
            roi_path = r["roi_norm_path"]
            mask_path = r["roi_mask_norm_path"]

            if data_type == "REAL":
                real_count += 1
            elif data_type == "SYNTH":
                synth_count += 1
            else:
                empty_count += 1

            has_roi = bool(roi_path and Path(roi_path).exists())
            has_mask = bool(mask_path and Path(mask_path).exists())

            if has_roi:
                roi_count += 1
            else:
                missing_roi.append(h)

            if has_mask:
                mask_count += 1
            else:
                missing_mask.append(h)

            method = r["synth_method"] or ""
            src_h = r["synth_src_h"]

            synth_info = ""
            if data_type == "SYNTH":
                if src_h is not None:
                    synth_info = f" | method={method} from={float(src_h):g}"
                else:
                    synth_info = f" | method={method}"

            rows_print.append(
                f"- {h:6.1f} m | {data_type:<5} | "
                f"roi={'YES' if has_roi else 'NO '} | "
                f"mask={'YES' if has_mask else 'NO '}"
                f"{synth_info}"
            )

    profile_dir = Path("data/tree_profiles") / tree_id
    profile_csv = profile_dir / "profile.csv"
    preview_png = profile_dir / "preview.png"

    print(f"\n=== PROJECT STATUS: {tree_id} ===\n")

    print("Levels summary:")
    print(f"- total grid levels : {total}")
    print(f"- REAL              : {real_count}")
    print(f"- SYNTH             : {synth_count}")
    print(f"- EMPTY/OTHER       : {empty_count}")

    print("\nFiles summary:")
    print(f"- levels with ROI   : {roi_count}/{total}")
    print(f"- levels with masks : {mask_count}/{total}")

    print("\nExport:")
    print(f"- profile.csv       : {'YES' if profile_csv.exists() else 'NO '}  {profile_csv}")
    print(f"- preview.png       : {'YES' if preview_png.exists() else 'NO '}  {preview_png}")

    print("\nDetailed levels:")
    for line in rows_print:
        print(line)

    if missing_roi:
        print("\nMissing ROI levels:")
        print(", ".join(str(x) for x in missing_roi))

    if missing_mask:
        print("\nMissing mask levels:")
        print(", ".join(str(x) for x in missing_mask))