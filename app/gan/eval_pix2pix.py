import math
import re
from pathlib import Path

import torch
from torch.utils.data import DataLoader

from app.gan.pix2pix_dataset import Pix2PixFolderDataset
from app.gan.pix2pix_models import GeneratorUNet

def denorm(x: torch.Tensor) -> torch.Tensor:
    return (x + 1.0) / 2.0

def mae(img1: torch.Tensor, img2: torch.Tensor) -> float:
    return float(torch.mean(torch.abs(img1 - img2)).item())

def psnr(img1: torch.Tensor, img2: torch.Tensor) -> float:
    mse = torch.mean((img1 - img2) ** 2).item()
    if mse <= 1e-12:
        return 99.0
    return 10.0 * math.log10(1.0 / mse)


def parse_heights_from_name(name: str):
    m = re.search(r"_A(\d+(?:\.\d+)?)_B(\d+(?:\.\d+)?)_", name)
    if not m:
        return None, None
    return float(m.group(1)), float(m.group(2))


@torch.no_grad()
def eval_pix2pix(
    data_root: Path,
    checkpoint_path: Path,
    out_dir: Path,
    device: str = "cpu",
):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "images").mkdir(parents=True, exist_ok=True)

    ds = Pix2PixFolderDataset(data_root, "test", image_size=256)
    dl = DataLoader(ds, batch_size=1, shuffle=False, num_workers=0)

    G = GeneratorUNet().to(device)

    state = torch.load(checkpoint_path, map_location=device)
    if isinstance(state, dict) and "state_dict" in state:
        G.load_state_dict(state["state_dict"])
    else:
        G.load_state_dict(state)

    G.eval()

    import torchvision.transforms.functional as TF
    from PIL import Image, ImageDraw, ImageFont

    mae_list = []
    psnr_list = []

    def get_font(size=18):
        try:
            return ImageFont.truetype("arial.ttf", size)
        except Exception:
            return ImageFont.load_default()

    font = get_font(18)

    for A, B, fn in dl:
        A = A.to(device)
        B = B.to(device)

        fakeB = G(A)

        A_img = denorm(A[0].cpu()).clamp(0, 1)
        B_img = denorm(B[0].cpu()).clamp(0, 1)
        F_img = denorm(fakeB[0].cpu()).clamp(0, 1)

        mae_list.append(mae(F_img, B_img))
        psnr_list.append(psnr(F_img, B_img))

        imgA = TF.to_pil_image(A_img)
        imgF = TF.to_pil_image(F_img)
        imgB = TF.to_pil_image(B_img)

        w, h = imgA.size
        canvas = Image.new("RGB", (w * 3, h))
        canvas.paste(imgA, (0, 0))
        canvas.paste(imgF, (w, 0))
        canvas.paste(imgB, (w * 2, 0))

        if isinstance(fn, (list, tuple)):
            name = fn[0]
        elif isinstance(fn, dict):
            if "name" in fn:
                name = fn["name"][0]
            elif "filename" in fn:
                name = fn["filename"][0]
            elif "fn" in fn:
                name = fn["fn"][0]
            else:
                k0 = next(iter(fn.keys()))
                v0 = fn[k0]
                name = v0[0] if isinstance(v0, (list, tuple)) else str(v0)
        else:
            name = str(fn)
        a_h, b_h = parse_heights_from_name(name)
        if (a_h is None or b_h is None) and isinstance(fn, dict):
            try:
                if "a_h" in fn and a_h is None:
                    a_h = float(fn["a_h"][0])
                if "b_h" in fn and b_h is None:
                    b_h = float(fn["b_h"][0])
            except Exception:
                pass

        pair_type = "unknown"
        if "_neighbor" in name:
            pair_type = "neighbor"
        elif "_far" in name:
            pair_type = "far"

        title = f"{name}"
        if a_h is not None and b_h is not None:
            title = f"A={a_h:g}m -> B={b_h:g}m | {pair_type}"

        draw = ImageDraw.Draw(canvas)
        pad = 6
        text_w, text_h = draw.textbbox((0, 0), title, font=font)[2:]
        draw.rectangle([0, 0, text_w + 2 * pad, text_h + 2 * pad], fill=(0, 0, 0))
        draw.text((pad, pad), title, font=font, fill=(255, 255, 255))

        name_str = str(name)

        name_str = Path(name_str).name

        if Path(name_str).suffix == "":
            name_str = name_str + ".png"

        name_str = name_str.replace(":", "_").replace("|", "_").replace("?", "_").replace("*", "_")

        out_path = out_dir / "images" / name_str

        canvas.save(out_path, format="PNG")

    mean_mae = sum(mae_list) / len(mae_list)
    mean_psnr = sum(psnr_list) / len(psnr_list)

    metrics_txt = out_dir / "metrics.txt"
    metrics_txt.write_text(
        f"checkpoint: {checkpoint_path}\n"
        f"n_test: {len(ds)}\n"
        f"MAE_mean: {mean_mae:.6f}\n"
        f"PSNR_mean: {mean_psnr:.3f}\n",
        encoding="utf-8"
    )

    return mean_mae, mean_psnr