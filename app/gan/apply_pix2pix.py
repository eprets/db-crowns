import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple

import torch
from PIL import Image
import torchvision.transforms as T

from app.db.connection import get_connection
from app.gan.pix2pix_models import GeneratorUNet


@dataclass
class LevelRow:
    h_level: float
    data_type: str           # "REAL" / "SYNTH"
    roi_norm_path: Optional[str]


def _sorted_levels(levels: List[float]) -> List[float]:
    return sorted([float(x) for x in levels])


def _find_neighbors(levels: List[float], target: float) -> Tuple[Optional[float], Optional[float]]:
    levels = _sorted_levels(levels)
    if target not in levels:
        return None, None
    i = levels.index(target)
    prev_h = levels[i - 1] if i - 1 >= 0 else None
    next_h = levels[i + 1] if i + 1 < len(levels) else None
    return prev_h, next_h


def _get_level(db_path: Path, tree_id: str, h_level: float) -> Optional[LevelRow]:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT h_level, data_type, roi_norm_path
            FROM crown_levels
            WHERE tree_id = ? AND h_level = ?
            LIMIT 1
            """,
            (tree_id, float(h_level)),
        )
        r = cur.fetchone()
        if r is None:
            return None
        return LevelRow(h_level=float(r["h_level"]), data_type=str(r["data_type"]), roi_norm_path=r["roi_norm_path"])


def _choose_src_neighbor(
    db_path: Path,
    tree_id: str,
    target_h: float,
    levels_grid: List[float],
    allow_synth_as_source: bool = False
) -> Optional[float]:
    prev_h, next_h = _find_neighbors(levels_grid, target_h)
    candidates: List[float] = []
    if prev_h is not None:
        candidates.append(prev_h)
    if next_h is not None:
        candidates.append(next_h)

    for h in candidates:
        row = _get_level(db_path, tree_id, h)
        if row is None:
            continue
        if not row.roi_norm_path:
            continue
        if row.data_type == "REAL":
            return h
        if allow_synth_as_source and row.data_type == "SYNTH":
            return h

    return None


def _load_image_tensor(path: Path, image_size: int = 256) -> torch.Tensor:
    img = Image.open(path).convert("RGB")
    tfm = T.Compose([
        T.Resize((image_size, image_size)),
        T.ToTensor(),                  # [0..1]
        T.Normalize((0.5,0.5,0.5), (0.5,0.5,0.5)),  # -> [-1..1]
    ])
    return tfm(img).unsqueeze(0)  # [1,C,H,W]


def _save_tensor_image(t: torch.Tensor, out_path: Path) -> None:
    if t.dim() == 4:
        t = t[0]
    t = (t * 0.5 + 0.5).clamp(0, 1)  # -> [0..1]
    img = T.ToPILImage()(t.cpu())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)


@torch.no_grad()
def apply_pix2pix_one(
    db_path: Path,
    tree_id: str,
    target_h: float,
    levels_grid: List[float],
    checkpoint_path: Path,
    roi_norm_dir: Path,
    src_h: Optional[float] = None,
    device: str = "cpu",
    allow_synth_as_source: bool = False,
) -> Optional[Path]:

    target_h = float(target_h)

    if src_h is None:
        src_h = _choose_src_neighbor(
            db_path=db_path,
            tree_id=tree_id,
            target_h=target_h,
            levels_grid=levels_grid,
            allow_synth_as_source=allow_synth_as_source
        )
        if src_h is None:
            logging.warning("No neighbor source for tree=%s target_h=%s", tree_id, target_h)
            return None
    else:
        src_h = float(src_h)

    src_row = _get_level(db_path, tree_id, src_h)
    if src_row is None or not src_row.roi_norm_path:
        logging.warning("Source level missing roi_norm: tree=%s src_h=%s", tree_id, src_h)
        return None

    src_path = Path(src_row.roi_norm_path)
    if not src_path.exists():
        logging.warning("Source roi_norm file not found: %s", src_path)
        return None

    G = GeneratorUNet().to(device)
    state = torch.load(checkpoint_path, map_location=device)
    if isinstance(state, dict) and "state_dict" in state:
        G.load_state_dict(state["state_dict"])
    else:
        G.load_state_dict(state)
    G.eval()

    A = _load_image_tensor(src_path, image_size=256).to(device)
    fakeB = G(A)

    out_path = roi_norm_dir / f"{tree_id}_{target_h:g}_pix2pix_from_{src_h:g}.png"
    _save_tensor_image(fakeB, out_path)

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO crown_levels (tree_id, h_level, data_type, roi_norm_path, synth_method, synth_src_h, mapping_error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tree_id, h_level) DO UPDATE SET
                data_type     = excluded.data_type,
                roi_norm_path = excluded.roi_norm_path,
                synth_method  = excluded.synth_method,
                synth_src_h   = excluded.synth_src_h
            """,
            (
                tree_id,
                target_h,
                "SYNTH",
                str(out_path),
                "pix2pix",
                float(src_h),
                0.0,
            ),
        )

        conn.commit()

    logging.info("Pix2Pix synth done: tree=%s A=%s -> B=%s saved=%s", tree_id, src_h, target_h, out_path)
    return out_path