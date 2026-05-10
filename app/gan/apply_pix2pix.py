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
    data_type: str
    roi_norm_path: Optional[str]


def _sorted_levels(levels: List[float]) -> List[float]:
    return sorted([float(x) for x in levels])


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

    return LevelRow(
        h_level=float(r["h_level"]),
        data_type=str(r["data_type"]),
        roi_norm_path=r["roi_norm_path"],
    )


def _level_is_usable(
    db_path: Path,
    tree_id: str,
    h_level: float,
    allow_synth_as_source: bool = False,
) -> bool:
    """
    Проверяет, можно ли использовать уровень как источник A для Pix2Pix.
    """
    row = _get_level(db_path, tree_id, h_level)

    if row is None:
        return False

    if not row.roi_norm_path:
        return False

    path = Path(row.roi_norm_path)
    if not path.exists():
        return False

    data_type = str(row.data_type).upper().strip()

    if data_type == "REAL":
        return True

    if allow_synth_as_source and data_type == "SYNTH":
        return True

    return False


def _choose_source_height(
    db_path: Path,
    tree_id: str,
    target_h: float,
    levels_grid: List[float],
    allow_synth_as_source: bool = False,
    allow_far: bool = False,
) -> Optional[float]:
    """
    Автоматически выбирает источник A для генерации B=target_h.

    Если allow_far=False:
        ищем только ближайших соседей по сетке: слева и справа.

    Если allow_far=True:
        если соседей нет, ищем дальше по сетке.

    REAL всегда предпочтительнее SYNTH.
    """
    levels = _sorted_levels(levels_grid)
    target_h = float(target_h)

    if target_h not in levels:
        logging.warning(
            "Target height %.1f is not in heights_grid. Check configs/config.yaml",
            target_h,
        )
        return None

    idx = levels.index(target_h)

    max_hops = 1
    if allow_far:
        max_hops = max(idx, len(levels) - 1 - idx)

    for hop in range(1, max_hops + 1):
        candidates = []

        left_idx = idx - hop
        right_idx = idx + hop

        if left_idx >= 0:
            candidates.append(levels[left_idx])

        if right_idx < len(levels):
            candidates.append(levels[right_idx])

        # Сначала ищем REAL
        for h in candidates:
            row = _get_level(db_path, tree_id, h)
            if row is not None and str(row.data_type).upper().strip() == "REAL":
                if _level_is_usable(db_path, tree_id, h, allow_synth_as_source=False):
                    return h

        # Потом, если разрешено, ищем SYNTH
        if allow_synth_as_source:
            for h in candidates:
                row = _get_level(db_path, tree_id, h)
                if row is not None and str(row.data_type).upper().strip() == "SYNTH":
                    if _level_is_usable(db_path, tree_id, h, allow_synth_as_source=True):
                        return h

    return None


def _load_image_tensor(path: Path, image_size: int = 256) -> torch.Tensor:
    img = Image.open(path).convert("RGB")

    tfm = T.Compose([
        T.Resize((image_size, image_size)),
        T.ToTensor(),
        T.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5)),
    ])

    return tfm(img).unsqueeze(0)


def _save_tensor_image(t: torch.Tensor, out_path: Path) -> None:
    if t.dim() == 4:
        t = t[0]

    t = (t * 0.5 + 0.5).clamp(0, 1)

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
    allow_far: bool = False,
    overwrite_real: bool = False,
) -> Optional[Path]:
    """
    Безопасный Pix2Pix-синтез одного уровня.

    По умолчанию:
    - REAL не перезаписываем;
    - если src_h указан, используем его;
    - если src_h не указан, ищем соседний источник;
    - allow_far=True разрешает искать источник дальше по сетке.
    """

    target_h = float(target_h)

    # 1. ЖЁСТКАЯ ЗАЩИТА REAL уровня
    # Если уровень уже REAL, по умолчанию его нельзя перезаписывать синтезом.
    target_row = _get_level(db_path, tree_id, target_h)

    if target_row is not None:
        target_type = str(target_row.data_type).upper().strip()

        if target_type == "REAL" and not overwrite_real:
            logging.warning(
                "SAFETY BLOCK: target level is REAL. "
                "tree=%s h=%.1f roi=%s. "
                "Pix2Pix synth is cancelled. "
                "Use --overwrite-real only if you intentionally want to replace REAL.",
                tree_id,
                target_h,
                target_row.roi_norm_path,
            )
            return None

    # 2. Выбор источника A
    if src_h is None:
        src_h = _choose_source_height(
            db_path=db_path,
            tree_id=tree_id,
            target_h=target_h,
            levels_grid=levels_grid,
            allow_synth_as_source=allow_synth_as_source,
            allow_far=allow_far,
        )

        if src_h is None:
            logging.warning(
                "No suitable source found for tree=%s target_h=%.1f. "
                "Try --allow-far or --src-h <height>.",
                tree_id,
                target_h,
            )
            return None
    else:
        src_h = float(src_h)

    # 3. Проверка источника
    src_row = _get_level(db_path, tree_id, src_h)

    if src_row is None or not src_row.roi_norm_path:
        logging.warning(
            "Source level missing roi_norm. tree=%s src_h=%.1f",
            tree_id,
            src_h,
        )
        return None

    src_path = Path(src_row.roi_norm_path)

    if not src_path.exists():
        logging.warning("Source roi_norm file not found: %s", src_path)
        return None

    # 4. Загрузка модели Pix2Pix
    G = GeneratorUNet().to(device)

    state = torch.load(checkpoint_path, map_location=device)

    if isinstance(state, dict) and "state_dict" in state:
        G.load_state_dict(state["state_dict"])
    else:
        G.load_state_dict(state)

    G.eval()

    # 5. Инференс
    A = _load_image_tensor(src_path, image_size=256).to(device)
    fakeB = G(A)

    # 6. Сохранение изображения
    out_path = roi_norm_dir / f"{tree_id}_{target_h:g}_pix2pix_from_{src_h:g}.png"

    _save_tensor_image(fakeB, out_path)

    # 7. Запись результата в БД
    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO crown_levels
            (tree_id, h_level, data_type, roi_norm_path, synth_method, synth_src_h, mapping_error)
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

    logging.info(
        "Pix2Pix synth done: tree=%s A=%.1f -> B=%.1f saved=%s",
        tree_id,
        src_h,
        target_h,
        out_path,
    )

    return out_path