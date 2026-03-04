import logging
from pathlib import Path
from pathlib import Path
import torch
import torchvision.transforms.functional as TF
from PIL import Image, ImageDraw, ImageFont
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from tqdm import tqdm

from app.gan.pix2pix_dataset import Pix2PixFolderDataset
from app.gan.pix2pix_models import GeneratorUNet, Discriminator


def denorm(x: torch.Tensor) -> torch.Tensor:
    return (x + 1.0) / 2.0

@torch.no_grad()
def save_preview(out_dir: Path, G, A, B, step: int, meta: dict | None = None):
    out_dir.mkdir(parents=True, exist_ok=True)

    fakeB = G(A)

    A_img = denorm(A[0].cpu()).clamp(0, 1)
    B_img = denorm(B[0].cpu()).clamp(0, 1)
    F_img = denorm(fakeB[0].cpu()).clamp(0, 1)

    imgA = TF.to_pil_image(A_img)
    imgF = TF.to_pil_image(F_img)
    imgB = TF.to_pil_image(B_img)

    w, h = imgA.size
    canvas = Image.new("RGB", (w * 3, h + 40), (0, 0, 0))
    canvas.paste(imgA, (0, 40))
    canvas.paste(imgF, (w, 40))
    canvas.paste(imgB, (w * 2, 40))

    draw = ImageDraw.Draw(canvas)

    try:
        font = ImageFont.truetype("arial.ttf", 16)
    except Exception:
        font = ImageFont.load_default()

    if meta:
        a_h = meta.get("a_h")
        b_h = meta.get("b_h")
        pair_type = meta.get("pair_type", "")
        text = f"A={a_h}m  ->  B={b_h}m   ({pair_type})   step={step}"
    else:
        text = f"step={step}"

    draw.text((10, 10), text, fill=(255, 255, 255), font=font)

    # имя файла с высотами (если meta есть)
    if meta:
        a_h = meta.get("a_h")
        b_h = meta.get("b_h")
        pair_type = meta.get("pair_type", "")
        fname = f"preview_step_{step:06d}_A{int(a_h)}_B{int(b_h)}_{pair_type}.png"
    else:
        fname = f"preview_step_{step:06d}.png"

    canvas.save(out_dir / fname)

def train_pix2pix(
    data_root: Path,
    out_dir: Path,
    epochs: int = 50,
    batch_size: int = 4,
    lr: float = 2e-4,
    lambda_l1: float = 100.0,
    device: str = "cuda" if torch.cuda.is_available() else "cpu",
):
    out_dir = out_dir.resolve()
    (out_dir / "checkpoints").mkdir(parents=True, exist_ok=True)
    (out_dir / "previews").mkdir(parents=True, exist_ok=True)

    logging.info("Using device: %s", device)

    ds_train = Pix2PixFolderDataset(data_root, "train", image_size=256)
    ds_val = Pix2PixFolderDataset(data_root, "val", image_size=256)

    dl_train = DataLoader(ds_train, batch_size=batch_size, shuffle=True, num_workers=0)
    dl_val = DataLoader(ds_val, batch_size=1, shuffle=True, num_workers=0)

    G = GeneratorUNet().to(device)
    D = Discriminator().to(device)

    criterion_gan = nn.BCEWithLogitsLoss()
    criterion_l1 = nn.L1Loss()

    opt_G = torch.optim.Adam(G.parameters(), lr=lr, betas=(0.5, 0.999))
    opt_D = torch.optim.Adam(D.parameters(), lr=lr, betas=(0.5, 0.999))

    step = 0

    for epoch in range(1, epochs + 1):
        G.train()
        D.train()

        pbar = tqdm(dl_train, desc=f"epoch {epoch}/{epochs}", leave=False)
        for A, B, _ in pbar:
            A = A.to(device)
            B = B.to(device)

            opt_D.zero_grad()

            fake_B = G(A).detach()

            pred_real = D(A, B)
            pred_fake = D(A, fake_B)

            loss_D_real = criterion_gan(pred_real, torch.ones_like(pred_real))
            loss_D_fake = criterion_gan(pred_fake, torch.zeros_like(pred_fake))
            loss_D = 0.5 * (loss_D_real + loss_D_fake)

            loss_D.backward()
            opt_D.step()

            opt_G.zero_grad()
            fake_B = G(A)

            pred_fake2 = D(A, fake_B)
            loss_G_gan = criterion_gan(pred_fake2, torch.ones_like(pred_fake2))
            loss_G_l1 = criterion_l1(fake_B, B)
            loss_G = loss_G_gan + lambda_l1 * loss_G_l1

            loss_G.backward()
            opt_G.step()

            step += 1
            pbar.set_postfix({
                "D": f"{loss_D.item():.3f}",
                "G": f"{loss_G.item():.3f}",
                "G_gan": f"{loss_G_gan.item():.3f}",
                "L1": f"{loss_G_l1.item():.3f}",
            })

            if step % 200 == 0:
                A_val, B_val, meta = next(iter(dl_val))
                save_preview(out_dir / "previews", G, A_val.to(device), B_val.to(device), step, meta=meta)
        torch.save(G.state_dict(), out_dir / "checkpoints" / f"G_epoch_{epoch:03d}.pt")
        torch.save(D.state_dict(), out_dir / "checkpoints" / f"D_epoch_{epoch:03d}.pt")

        logging.info("Epoch %d done. Saved checkpoints.", epoch)

    logging.info("Training finished. Output: %s", out_dir)