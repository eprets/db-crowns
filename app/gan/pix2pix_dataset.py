import csv
from pathlib import Path
from typing import Dict, Tuple

from PIL import Image
import torch
from torch.utils.data import Dataset
import torchvision.transforms as T


class Pix2PixFolderDataset(Dataset):

    def __init__(self, data_root: Path, split: str, image_size: int = 256):
        self.data_root = Path(data_root)
        self.split = split

        self.csv_path = self.data_root / "split.csv"
        if not self.csv_path.exists():
            raise FileNotFoundError(f"split.csv not found: {self.csv_path}")

        self.rows = []

        # Новый формат split.csv:
        # delimiter=";"
        # есть заголовки:
        # pair_id;tree_id;h_a;h_b;gap;a_path;b_path;...;split;a_split_path;b_split_path
        with self.csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(2048)
            f.seek(0)

            if ";" in sample.splitlines()[0]:
                reader = csv.DictReader(f, delimiter=";")

                for row in reader:
                    if row.get("split") != split:
                        continue

                    self.rows.append({
                        "pair_id": row.get("pair_id", ""),
                        "pair_type": row.get("direction", "unknown"),
                        "a_h": float(row.get("h_a", 0)),
                        "b_h": float(row.get("h_b", 0)),
                        "a_path": self.data_root / row["a_split_path"],
                        "b_path": self.data_root / row["b_split_path"],
                    })

            else:
                # Старый формат split.csv без заголовков
                reader = csv.reader(f)

                for r in reader:
                    if len(r) < 8:
                        continue
                    if r[1] != split:
                        continue

                    pair_id = r[0]
                    pair_type = r[3]
                    a_h = float(r[4])
                    b_h = float(r[5])
                    a_rel = r[6]
                    b_rel = r[7]

                    self.rows.append({
                        "pair_id": pair_id,
                        "pair_type": pair_type,
                        "a_h": a_h,
                        "b_h": b_h,
                        "a_path": self.data_root / self.split / a_rel,
                        "b_path": self.data_root / self.split / b_rel,
                    })

        self.tf = T.Compose([
            T.Resize((image_size, image_size)),
            T.ToTensor(),
            T.Normalize(
                (0.5, 0.5, 0.5),
                (0.5, 0.5, 0.5)
            ),
        ])

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor, Dict]:
        row = self.rows[idx]

        a_path = Path(row["a_path"])
        b_path = Path(row["b_path"])

        A_img = Image.open(a_path).convert("RGB")
        B_img = Image.open(b_path).convert("RGB")

        A = self.tf(A_img)
        B = self.tf(B_img)

        meta = {
            "pair_id": row["pair_id"],
            "pair_type": row["pair_type"],
            "a_h": row["a_h"],
            "b_h": row["b_h"],
            "a_path": str(a_path),
            "b_path": str(b_path),
        }

        return A, B, meta