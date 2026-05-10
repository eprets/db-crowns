# app/split_training_dataset.py

import csv
import random
import shutil
from pathlib import Path
from typing import Dict, Any, List


def split_training_dataset(
    base_dir: Path,
    out_dir: Path,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
    test_ratio: float = 0.1,
    seed: int = 42,
) -> Dict[str, Any]:
    """
    Делит Pix2Pix dataset на train / val / test.

    Ожидает:
      base_dir/
        A/
        B/
        pairs.csv

    Создаёт:
      out_dir/
        train/A
        train/B
        val/A
        val/B
        test/A
        test/B
        split.csv
    """

    if abs((train_ratio + val_ratio + test_ratio) - 1.0) > 1e-6:
        raise ValueError("train_ratio + val_ratio + test_ratio must be 1.0")

    pairs_csv = base_dir / "pairs.csv"

    if not pairs_csv.exists():
        raise FileNotFoundError(f"pairs.csv not found: {pairs_csv}")

    if out_dir.exists():
        shutil.rmtree(out_dir)

    for split in ["train", "val", "test"]:
        (out_dir / split / "A").mkdir(parents=True, exist_ok=True)
        (out_dir / split / "B").mkdir(parents=True, exist_ok=True)

    rows: List[dict] = []

    with pairs_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            rows.append(row)

    random.seed(seed)
    random.shuffle(rows)

    n = len(rows)
    n_train = int(n * train_ratio)
    n_val = int(n * val_ratio)

    train_rows = rows[:n_train]
    val_rows = rows[n_train:n_train + n_val]
    test_rows = rows[n_train + n_val:]

    split_map = {
        "train": train_rows,
        "val": val_rows,
        "test": test_rows,
    }

    split_csv_rows = []

    for split_name, split_rows in split_map.items():
        for row in split_rows:
            a_src = base_dir / row["a_path"]
            b_src = base_dir / row["b_path"]

            a_name = Path(row["a_path"]).name
            b_name = Path(row["b_path"]).name

            a_dst = out_dir / split_name / "A" / a_name
            b_dst = out_dir / split_name / "B" / b_name

            if not a_src.exists():
                raise FileNotFoundError(f"A file not found: {a_src}")

            if not b_src.exists():
                raise FileNotFoundError(f"B file not found: {b_src}")

            shutil.copy2(a_src, a_dst)
            shutil.copy2(b_src, b_dst)

            row_out = dict(row)
            row_out["split"] = split_name
            row_out["a_split_path"] = str(Path(split_name) / "A" / a_name)
            row_out["b_split_path"] = str(Path(split_name) / "B" / b_name)

            split_csv_rows.append(row_out)

    split_csv = out_dir / "split.csv"

    fieldnames = list(split_csv_rows[0].keys()) if split_csv_rows else []

    with split_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(split_csv_rows)

    return {
        "out_dir": str(out_dir),
        "split_csv": str(split_csv),
        "train": len(train_rows),
        "val": len(val_rows),
        "test": len(test_rows),
        "total": n,
    }