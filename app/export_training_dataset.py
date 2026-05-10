# app/export_training_dataset.py

import csv
import shutil
from pathlib import Path
from typing import Dict, Any

from app.db.connection import get_connection


def _safe_h(h: float) -> str:
    if float(h).is_integer():
        return str(int(h))
    return str(h).replace(".", "_")


def export_training_dataset(
    db_path: Path,
    out_dir: Path,
    neighbor_max_gap_m: float = 25.0,
    include_reverse: bool = True,
    real_only: bool = True,
) -> Dict[str, Any]:
    """
    Экспортирует пары A->B для обучения Pix2Pix.

    A = ROI кроны на одной высоте.
    B = ROI кроны на другой высоте.

    По умолчанию используем только REAL->REAL,
    чтобы обучающий набор был чище.
    """

    if out_dir.exists():
        shutil.rmtree(out_dir)

    a_dir = out_dir / "A"
    b_dir = out_dir / "B"

    a_dir.mkdir(parents=True, exist_ok=True)
    b_dir.mkdir(parents=True, exist_ok=True)

    pairs_csv = out_dir / "pairs.csv"

    rows_for_csv = []

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT DISTINCT tree_id
            FROM crown_levels
            ORDER BY tree_id
            """
        )

        trees = [r["tree_id"] for r in cur.fetchall()]

        pair_idx = 0

        for tree_id in trees:
            cur.execute(
                """
                SELECT
                    tree_id,
                    h_level,
                    data_type,
                    roi_norm_path
                FROM crown_levels
                WHERE tree_id = ?
                  AND roi_norm_path IS NOT NULL
                ORDER BY h_level
                """,
                (tree_id,),
            )

            levels = [dict(r) for r in cur.fetchall()]

            if real_only:
                levels = [
                    r for r in levels
                    if str(r["data_type"]).upper().strip() == "REAL"
                ]

            for i in range(len(levels)):
                for j in range(i + 1, len(levels)):
                    a = levels[i]
                    b = levels[j]

                    h_a = float(a["h_level"])
                    h_b = float(b["h_level"])

                    gap = abs(h_b - h_a)

                    if gap > neighbor_max_gap_m:
                        continue

                    src_a = Path(a["roi_norm_path"])
                    src_b = Path(b["roi_norm_path"])

                    if not src_a.exists() or not src_b.exists():
                        continue

                    pair_idx += 1

                    pair_id = (
                        f"{pair_idx:08d}_"
                        f"{tree_id}_"
                        f"A{_safe_h(h_a)}_B{_safe_h(h_b)}"
                    )

                    a_name = pair_id + ".png"
                    b_name = pair_id + ".png"

                    shutil.copy2(src_a, a_dir / a_name)
                    shutil.copy2(src_b, b_dir / b_name)

                    rows_for_csv.append({
                        "pair_id": pair_id,
                        "tree_id": tree_id,
                        "h_a": h_a,
                        "h_b": h_b,
                        "gap": gap,
                        "a_path": str(Path("A") / a_name),
                        "b_path": str(Path("B") / b_name),
                        "a_type": a["data_type"],
                        "b_type": b["data_type"],
                        "direction": "forward",
                    })

                    if include_reverse:
                        pair_idx += 1

                        pair_id_rev = (
                            f"{pair_idx:08d}_"
                            f"{tree_id}_"
                            f"A{_safe_h(h_b)}_B{_safe_h(h_a)}_rev"
                        )

                        a_name_rev = pair_id_rev + ".png"
                        b_name_rev = pair_id_rev + ".png"

                        shutil.copy2(src_b, a_dir / a_name_rev)
                        shutil.copy2(src_a, b_dir / b_name_rev)

                        rows_for_csv.append({
                            "pair_id": pair_id_rev,
                            "tree_id": tree_id,
                            "h_a": h_b,
                            "h_b": h_a,
                            "gap": gap,
                            "a_path": str(Path("A") / a_name_rev),
                            "b_path": str(Path("B") / b_name_rev),
                            "a_type": b["data_type"],
                            "b_type": a["data_type"],
                            "direction": "reverse",
                        })

    with pairs_csv.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "pair_id",
                "tree_id",
                "h_a",
                "h_b",
                "gap",
                "a_path",
                "b_path",
                "a_type",
                "b_type",
                "direction",
            ],
            delimiter=";",
        )

        writer.writeheader()
        writer.writerows(rows_for_csv)

    return {
        "out_dir": str(out_dir),
        "pairs_csv": str(pairs_csv),
        "total_pairs": len(rows_for_csv),
    }