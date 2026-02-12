import csv
import logging
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import cv2
import numpy as np

from app.db.connection import get_connection


def read_image_unicode(path: str):
    data = np.fromfile(path, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    return img


def save_image_unicode_png(path: Path, img) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ok, buf = cv2.imencode(".png", img)
    if not ok:
        raise RuntimeError(f"Cannot encode image for saving: {path}")
    buf.tofile(str(path))


@dataclass
class PairItem:
    tree_id: str
    h_in: float
    h_out: float
    src_in: str
    src_out: str


def _make_neighbor_pairs(levels_sorted: List[float]) -> List[Tuple[float, float]]:
    """
    Пары по соседним уровням сетки:
    [0,5,10,15] -> (0,5), (5,10), (10,15)
    """
    pairs = []
    for i in range(len(levels_sorted) - 1):
        pairs.append((float(levels_sorted[i]), float(levels_sorted[i + 1])))
    return pairs


def export_pix2pix_pairs(
    db_path: Path,
    out_dir: Path,
    levels_grid: List[float],
    pair_mode: str = "neighbors",     # сейчас делаем только neighbors
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
    test_ratio: float = 0.1,
    seed: int = 42,
    only_tree_id: Optional[str] = None,
) -> int:
    """
    Экспортирует Pix2Pix датасет (пары A->B) из crown_levels.

    Берём только REAL уровни:
      crown_levels.data_type='real'
      roi_norm_path != NULL

    Разбиение делаем по tree_id (чтобы один и тот же tree_id не попадал в train и val/test).

    Выход:
      out_dir/train/A/*.png
      out_dir/train/B/*.png
      out_dir/val/A/*.png
      out_dir/val/B/*.png
      out_dir/test/A/*.png
      out_dir/test/B/*.png
      out_dir/manifest.csv

    Возвращает число экспортированных пар.
    """
    # проверки долей
    s = train_ratio + val_ratio + test_ratio
    if abs(s - 1.0) > 1e-6:
        raise ValueError("train_ratio + val_ratio + test_ratio must be 1.0")

    out_dir = out_dir.resolve()
    (out_dir / "train" / "A").mkdir(parents=True, exist_ok=True)
    (out_dir / "train" / "B").mkdir(parents=True, exist_ok=True)
    (out_dir / "val" / "A").mkdir(parents=True, exist_ok=True)
    (out_dir / "val" / "B").mkdir(parents=True, exist_ok=True)
    (out_dir / "test" / "A").mkdir(parents=True, exist_ok=True)
    (out_dir / "test" / "B").mkdir(parents=True, exist_ok=True)

    levels_sorted = sorted([float(x) for x in levels_grid])

    if pair_mode != "neighbors":
        raise ValueError("Currently supported pair_mode='neighbors' only")

    neighbor_pairs = _make_neighbor_pairs(levels_sorted)

    # 1) вытаскиваем REAL roi_norm для всех деревьев
    # соберём: tree_id -> {h_level -> roi_norm_path}
    tree_map: Dict[str, Dict[float, str]] = {}

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        if only_tree_id:
            cur.execute(
                """
                SELECT tree_id, h_level, roi_norm_path
                FROM crown_levels
                WHERE data_type='real'
                  AND roi_norm_path IS NOT NULL
                  AND tree_id = ?
                """,
                (only_tree_id,),
            )
        else:
            cur.execute(
                """
                SELECT tree_id, h_level, roi_norm_path
                FROM crown_levels
                WHERE data_type='real'
                  AND roi_norm_path IS NOT NULL
                """
            )

        rows = cur.fetchall()
        for r in rows:
            tid = r["tree_id"]
            h = float(r["h_level"])
            p = r["roi_norm_path"]
            tree_map.setdefault(tid, {})[h] = p

    tree_ids = sorted(tree_map.keys())
    if not tree_ids:
        logging.warning("No REAL roi_norm data found in crown_levels. Nothing to export.")
        return 0

    # 2) формируем список всех пар (по каждому дереву)
    all_pairs: List[PairItem] = []
    for tid in tree_ids:
        have = tree_map[tid]
        for (h_in, h_out) in neighbor_pairs:
            if h_in in have and h_out in have:
                all_pairs.append(
                    PairItem(
                        tree_id=tid,
                        h_in=h_in,
                        h_out=h_out,
                        src_in=have[h_in],
                        src_out=have[h_out],
                    )
                )

    if not all_pairs:
        logging.warning("No neighbor pairs found (need REAL on both heights). Nothing to export.")
        return 0

    # 3) split по tree_id (без утечки)
    rng = random.Random(seed)
    rng.shuffle(tree_ids)

    n = len(tree_ids)
    n_train = int(round(n * train_ratio))
    n_val = int(round(n * val_ratio))
    # test = остаток
    n_test = n - n_train - n_val

    train_ids = set(tree_ids[:n_train])
    val_ids = set(tree_ids[n_train:n_train + n_val])
    test_ids = set(tree_ids[n_train + n_val:])

    logging.info("Split trees: train=%d val=%d test=%d (total=%d)", len(train_ids), len(val_ids), len(test_ids), n)

    # 4) экспорт файлов
    exported = 0
    manifest_path = out_dir / "manifest.csv"

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["split", "tree_id", "h_in", "h_out", "A_path", "B_path", "src_A", "src_B"])

        for item in all_pairs:
            if item.tree_id in train_ids:
                split = "train"
            elif item.tree_id in val_ids:
                split = "val"
            else:
                split = "test"

            # имя файла: tree_001_15_to_25.png
            h_in_tag = int(item.h_in) if float(item.h_in).is_integer() else item.h_in
            h_out_tag = int(item.h_out) if float(item.h_out).is_integer() else item.h_out
            filename = f"{item.tree_id}_{h_in_tag}_to_{h_out_tag}.png"

            out_a = out_dir / split / "A" / filename
            out_b = out_dir / split / "B" / filename

            img_a = read_image_unicode(item.src_in)
            img_b = read_image_unicode(item.src_out)

            if img_a is None or img_b is None:
                logging.warning("Skip pair (cannot read): %s", filename)
                continue

            # (опционально) гарантируем одинаковый размер
            # но у тебя roi_norm уже 256x256 — просто на всякий случай
            if img_a.shape[:2] != img_b.shape[:2]:
                logging.warning("Skip pair (size mismatch): %s A=%s B=%s", filename, img_a.shape, img_b.shape)
                continue

            save_image_unicode_png(out_a, img_a)
            save_image_unicode_png(out_b, img_b)

            w.writerow([split, item.tree_id, item.h_in, item.h_out, str(out_a), str(out_b), item.src_in, item.src_out])
            exported += 1

    logging.info("Export done. Exported pairs=%d  manifest=%s", exported, str(manifest_path))
    return exported
