import csv
import logging
import random
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Tuple


@dataclass
class PairRow:
    pair_id: str
    tree_id: str
    A_file: str
    B_file: str
    kind: str
    A_height: float
    B_height: float


def _read_pairs_csv(pairs_csv: Path) -> List[PairRow]:
    rows: List[PairRow] = []
    with pairs_csv.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for d in r:
            rows.append(
                PairRow(
                    pair_id=d["pair_id"],
                    tree_id=d["tree_id"],
                    A_file=d["A_file"],
                    B_file=d["B_file"],
                    kind=d["kind"],
                    A_height=float(d["A_height"]),
                    B_height=float(d["B_height"]),
                )
            )
    return rows


def _copy_pair(base_dir: Path, out_split_dir: Path, split_name: str, pair: PairRow) -> None:
    src_A = base_dir / pair.A_file
    src_B = base_dir / pair.B_file

    dst_A = out_split_dir / split_name / "A" / src_A.name
    dst_B = out_split_dir / split_name / "B" / src_B.name

    dst_A.parent.mkdir(parents=True, exist_ok=True)
    dst_B.parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(src_A, dst_A)
    shutil.copy2(src_B, dst_B)


def split_pix2pix_dataset(
    base_dir: Path,
    out_dir: Path,
    train_ratio: float,
    val_ratio: float,
    test_ratio: float,
    seed: int = 42,
) -> Dict[str, Any]:

    base_dir = base_dir.resolve()
    out_dir = out_dir.resolve()

    pairs_csv = base_dir / "pairs.csv"
    if not pairs_csv.exists():
        raise RuntimeError(f"pairs.csv not found: {pairs_csv}")

    pairs = _read_pairs_csv(pairs_csv)
    if not pairs:
        raise RuntimeError("pairs.csv is empty")

    s = train_ratio + val_ratio + test_ratio
    if abs(s - 1.0) > 1e-6:
        raise ValueError("train_ratio + val_ratio + test_ratio must be 1.0")

    random.seed(seed)
    random.shuffle(pairs)

    n = len(pairs)
    n_train = int(n * train_ratio)
    n_val = int(n * val_ratio)
    n_test = n - n_train - n_val

    train_pairs = pairs[:n_train]
    val_pairs = pairs[n_train:n_train + n_val]
    test_pairs = pairs[n_train + n_val:]

    for p in train_pairs:
        _copy_pair(base_dir, out_dir, "train", p)
    for p in val_pairs:
        _copy_pair(base_dir, out_dir, "val", p)
    for p in test_pairs:
        _copy_pair(base_dir, out_dir, "test", p)

    # сохранить split.csv
    split_csv = out_dir / "split.csv"
    with split_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pair_id", "split", "tree_id", "kind", "A_height", "B_height", "A_file", "B_file"])
        for p in train_pairs:
            w.writerow([p.pair_id, "train", p.tree_id, p.kind, p.A_height, p.B_height, p.A_file, p.B_file])
        for p in val_pairs:
            w.writerow([p.pair_id, "val", p.tree_id, p.kind, p.A_height, p.B_height, p.A_file, p.B_file])
        for p in test_pairs:
            w.writerow([p.pair_id, "test", p.tree_id, p.kind, p.A_height, p.B_height, p.A_file, p.B_file])

    logging.info("Split done. train=%d val=%d test=%d out_dir=%s", n_train, n_val, n_test, out_dir)

    return {
        "out_dir": str(out_dir),
        "pairs_total": n,
        "train": n_train,
        "val": n_val,
        "test": n_test,
        "split_csv": str(split_csv),
    }