# app/gan/infer_from_checkpoint.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

import torch
from torch.utils.data import DataLoader

from PIL import Image, ImageDraw, ImageFont
import torchvision.transforms.functional as TF

from app.gan.pix2pix_models import GeneratorUNet

from app.gan.pix2pix_dataset import Pix2PixFolderDataset


def _meta_get_first(meta: Dict[str, Any], key: str, default=None):

    if meta is None:
        return default
    v = meta.get(key, default)
    if isinstance(v, (list, tuple)) and len(v) > 0:
        return v[0]
    return v

def _denorm(x: torch.Tensor) -> torch.Tensor:
    return (x + 1.0) / 2.0

@torch.no_grad()
def save_preview_triplet(
    out_dir: Path,
    A: torch.Tensor,
    fakeB: torch.Tensor,
    B: torch.Tensor,
    idx: int,
    meta: Dict[str, Any] | None = None,
):

    out_dir.mkdir(parents=True, exist_ok=True)

    A_img = _denorm(A[0].cpu()).clamp(0, 1)
    F_img = _denorm(fakeB[0].cpu()).clamp(0, 1)
    B_img = _denorm(B[0].cpu()).clamp(0, 1)

    imgA = TF.to_pil_image(A_img)
    imgF = TF.to_pil_image(F_img)
    imgB = TF.to_pil_image(B_img)

    w, h = imgA.size
    top_h = 40
    canvas = Image.new("RGB", (w * 3, h + top_h), (0, 0, 0))
    canvas.paste(imgA, (0, top_h))
    canvas.paste(imgF, (w, top_h))
    canvas.paste(imgB, (w * 2, top_h))

    draw = ImageDraw.Draw(canvas)
    try:
        font = ImageFont.truetype("arial.ttf", 16)
    except Exception:
        font = ImageFont.load_default()

    a_h = _meta_get_first(meta, "a_h", "?")
    b_h = _meta_get_first(meta, "b_h", "?")
    pair_type = _meta_get_first(meta, "pair_type", "")
    pair_id = _meta_get_first(meta, "pair_id", "")

    text = f"{pair_id} | A={a_h}m -> B={b_h}m ({pair_type})"
    draw.text((10, 10), text, fill=(255, 255, 255), font=font)

    # аккуратное имя файла
    try:
        a_int = int(float(a_h))
    except Exception:
        a_int = a_h
    try:
        b_int = int(float(b_h))
    except Exception:
        b_int = b_h

    safe_pair_type = str(pair_type).replace("/", "_").replace("\\", "_").replace(" ", "")
    fname = f"{idx:04d}_A{a_int}_B{b_int}_{safe_pair_type}.png"
    canvas.save(out_dir / fname)

def make_previews_from_checkpoint(
    pix2pix_split_dir: Path,
    checkpoint_path: Path,
    out_dir: Path,
    num_samples: int = 20,
    device: str = "cpu",
    image_size: int = 256,
):

    pix2pix_split_dir = Path(pix2pix_split_dir)
    checkpoint_path = Path(checkpoint_path)
    out_dir = Path(out_dir)

    if not checkpoint_path.exists():
        raise FileNotFoundError(f"Checkpoint not found: {checkpoint_path}")

    logging.info("Infer: split_dir=%s", pix2pix_split_dir)
    logging.info("Infer: checkpoint=%s", checkpoint_path)
    logging.info("Infer: out_dir=%s", out_dir)
    logging.info("Infer: device=%s", device)

    ds_test = Pix2PixFolderDataset(
        data_root=pix2pix_split_dir,
        split="test",
        image_size=image_size,
    )
    if len(ds_test) == 0:
        raise RuntimeError("Test dataset is empty. Check pix2pix_split/test and split.csv")

    dl = DataLoader(ds_test, batch_size=1, shuffle=True, num_workers=0)

    G = GeneratorUNet(in_channels=3, out_channels=3)  # <-- параметры должны совпадать с обучением!
    state = torch.load(checkpoint_path, map_location="cpu")

    if isinstance(state, dict) and "state_dict" in state and isinstance(state["state_dict"], dict):
        G.load_state_dict(state["state_dict"])
    else:
        G.load_state_dict(state)

    G.eval()
    G.to(device)

    saved = 0
    for A, B, meta in dl:
        A = A.to(device)
        B = B.to(device)

        fakeB = G(A)
        save_preview_triplet(
            out_dir=out_dir,
            A=A,
            fakeB=fakeB,
            B=B,
            idx=saved + 1,
            meta=meta,
        )
        saved += 1
        if saved >= num_samples:
            break

    logging.info("Infer done. Saved %d previews to %s", saved, out_dir)
    return saved