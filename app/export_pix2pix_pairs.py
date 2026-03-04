import csv
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

from app.db.connection import get_connection


@dataclass
class LevelRow:
    tree_id: str
    h_level: float
    data_type: str
    roi_norm_path: str
    synth_method: Optional[str]


def _fetch_levels_for_tree(db_path: Path, tree_id: str, include_synth: bool) -> List[LevelRow]:
    with get_connection(db_path) as conn:
        cur = conn.cursor()

        if include_synth:
            cur.execute(
                """
                SELECT tree_id, h_level, data_type, roi_norm_path, synth_method
                FROM crown_levels
                WHERE tree_id = ?
                  AND roi_norm_path IS NOT NULL
                ORDER BY h_level ASC
                """,
                (tree_id,),
            )
        else:
            cur.execute(
                """
                SELECT tree_id, h_level, data_type, roi_norm_path, synth_method
                FROM crown_levels
                WHERE tree_id = ?
                  AND roi_norm_path IS NOT NULL
                  AND (LOWER(data_type) = 'real')
                ORDER BY h_level ASC
                """,
                (tree_id,),
            )

        rows = cur.fetchall()

    out: List[LevelRow] = []
    for r in rows:
        out.append(
            LevelRow(
                tree_id=str(r["tree_id"]),
                h_level=float(r["h_level"]),
                data_type=str(r["data_type"]),
                roi_norm_path=str(r["roi_norm_path"]),
                synth_method=(str(r["synth_method"]) if "synth_method" in r.keys() and r["synth_method"] is not None else None),
            )
        )
    return out


def _fetch_all_tree_ids(db_path: Path) -> List[str]:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT tree_id FROM trees ORDER BY tree_id ASC")
        rows = cur.fetchall()
    return [str(r["tree_id"]) for r in rows]


def _ensure_empty_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _pair_kind(dh: float, neighbor_max: float, far_min: float, far_max: float) -> Optional[str]:
    if dh <= neighbor_max:
        return "neighbor"
    if far_min <= dh <= far_max:
        return "far"
    return None


def export_pix2pix_pairs(
    db_path: Path,
    out_dir: Path,
    neighbor_max_gap_m: float,
    far_min_gap_m: float,
    far_max_gap_m: float,
    include_reverse: bool,
    max_pairs_per_tree: int,
    include_synth: bool,
    image_ext: str = ".png",
) -> Dict[str, Any]:

    out_dir = out_dir.resolve()
    dir_A = out_dir / "A"
    dir_B = out_dir / "B"
    _ensure_empty_dir(dir_A)
    _ensure_empty_dir(dir_B)

    pairs_csv = out_dir / "pairs.csv"

    tree_ids = _fetch_all_tree_ids(db_path)
    if not tree_ids:
        raise RuntimeError("No trees in DB. Annotate at least one tree first.")

    total_pairs = 0
    per_tree_counts: Dict[str, int] = {}

    with pairs_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "pair_id",
            "tree_id",
            "A_height",
            "B_height",
            "kind",
            "A_file",
            "B_file",
            "A_data_type",
            "B_data_type",
        ])

        pair_idx = 0

        for tree_id in tree_ids:
            levels = _fetch_levels_for_tree(db_path, tree_id, include_synth=include_synth)

            # нужен минимум 2 уровня
            if len(levels) < 2:
                continue

            count_for_tree = 0

            for i in range(len(levels)):
                for j in range(i + 1, len(levels)):
                    hi = levels[i]
                    hj = levels[j]
                    dh = abs(hj.h_level - hi.h_level)

                    kind = _pair_kind(dh, neighbor_max_gap_m, far_min_gap_m, far_max_gap_m)
                    if kind is None:
                        continue

                    # создаём A->B
                    pair_idx += 1
                    pair_id = f"{pair_idx:08d}_{tree_id}_A{int(hi.h_level)}_B{int(hj.h_level)}_{kind}"

                    src_A = Path(hi.roi_norm_path)
                    src_B = Path(hj.roi_norm_path)

                    if not src_A.exists() or not src_B.exists():
                        logging.warning("Skip pair (missing file): %s or %s", src_A, src_B)
                        continue

                    dst_A = dir_A / f"{pair_id}{image_ext}"
                    dst_B = dir_B / f"{pair_id}{image_ext}"

                    _safe_copy(src_A, dst_A)
                    _safe_copy(src_B, dst_B)

                    w.writerow([
                        pair_id,
                        tree_id,
                        hi.h_level,
                        hj.h_level,
                        kind,
                        str(dst_A.relative_to(out_dir)).replace("\\", "/"),
                        str(dst_B.relative_to(out_dir)).replace("\\", "/"),
                        hi.data_type,
                        hj.data_type,
                    ])

                    total_pairs += 1
                    count_for_tree += 1

                    if count_for_tree >= max_pairs_per_tree:
                        break

                    if include_reverse:
                        pair_idx += 1
                        pair_id2 = f"{pair_idx:08d}_{tree_id}_A{int(hj.h_level)}_B{int(hi.h_level)}_{kind}_rev"

                        dst_A2 = dir_A / f"{pair_id2}{image_ext}"
                        dst_B2 = dir_B / f"{pair_id2}{image_ext}"

                        _safe_copy(src_B, dst_A2)  # теперь A=верхний уровень
                        _safe_copy(src_A, dst_B2)  # B=нижний уровень

                        w.writerow([
                            pair_id2,
                            tree_id,
                            hj.h_level,
                            hi.h_level,
                            kind,
                            str(dst_A2.relative_to(out_dir)).replace("\\", "/"),
                            str(dst_B2.relative_to(out_dir)).replace("\\", "/"),
                            hj.data_type,
                            hi.data_type,
                        ])

                        total_pairs += 1
                        count_for_tree += 1

                        if count_for_tree >= max_pairs_per_tree:
                            break

                if count_for_tree >= max_pairs_per_tree:
                    break

            if count_for_tree > 0:
                per_tree_counts[tree_id] = count_for_tree
                logging.info("Pix2Pix export: tree_id=%s pairs=%d", tree_id, count_for_tree)

    logging.info("Pix2Pix export done. total_pairs=%d out_dir=%s", total_pairs, out_dir)
    return {
        "out_dir": str(out_dir),
        "total_pairs": total_pairs,
        "per_tree_counts": per_tree_counts,
        "pairs_csv": str(pairs_csv),
    }