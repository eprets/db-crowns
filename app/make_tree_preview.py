# app/make_tree_preview.py

from pathlib import Path
from typing import List

from PIL import Image, ImageDraw


CELL_W = 260
CELL_H = 320

IMG_SIZE = 256
PADDING = 10


def _load_or_blank(path: Path | None, size=(256, 256), mode="RGB"):
    if path is None or not path.exists():
        if mode == "RGB":
            return Image.new("RGB", size, (40, 40, 40))
        return Image.new("L", size, 0)

    img = Image.open(path)

    if mode == "RGB":
        img = img.convert("RGB")
    else:
        img = img.convert("L")

    img = img.resize(size)

    return img


def make_tree_preview(
    tree_profile_dir: Path,
):
    """
    Создаёт preview.png для дерева.

    Ожидает структуру:
      tree_profile/
        images/
        masks/
    """

    images_dir = tree_profile_dir / "images"
    masks_dir = tree_profile_dir / "masks"

    image_files: List[Path] = sorted(images_dir.glob("*.png"))

    rows = []

    for img_path in image_files:
        stem = img_path.stem

        mask_name = stem + "_mask.png"
        mask_path = masks_dir / mask_name

        rows.append({
            "img": img_path,
            "mask": mask_path if mask_path.exists() else None,
            "name": stem,
        })

    # ---------- GRID LAYOUT ----------

    n = len(rows)

    COLS = 5
    ROWS = (n + COLS - 1) // COLS

    canvas_w = CELL_W * COLS
    canvas_h = CELL_H * ROWS

    canvas = Image.new(
        "RGB",
        (canvas_w, canvas_h),
        (20, 20, 20)
    )

    draw = ImageDraw.Draw(canvas)

    for i, row in enumerate(rows):
        col = i % COLS
        row_idx = i // COLS

        x0 = col * CELL_W
        y0 = row_idx * CELL_H

        # ---------- IMAGE ----------
        img = _load_or_blank(row["img"], size=(IMG_SIZE, IMG_SIZE), mode="RGB")

        canvas.paste(
            img,
            (x0 + PADDING, y0 + 40)
        )

        # ---------- MASK ----------
        mask = _load_or_blank(
            row["mask"],
            size=(IMG_SIZE, 40),
            mode="L"
        )

        mask_rgb = Image.merge("RGB", (mask, mask, mask))

        canvas.paste(
            mask_rgb,
            (x0 + PADDING, y0 + 40 + IMG_SIZE + 5)
        )

        # ---------- TEXT ----------
        draw.text(
            (x0 + PADDING, y0 + 10),
            row["name"],
            fill=(255, 255, 255)
        )

    out_path = tree_profile_dir / "preview.png"

    canvas.save(out_path)

    return out_path