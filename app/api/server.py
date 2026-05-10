# app/api/server.py

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from app.db.connection import get_connection


DB_PATH = Path("data/db/crowns.sqlite3")

app = FastAPI(
    title="Crown DB API",
    version="0.0.1",
)


@app.get("/")
def root():
    return {
        "project": "Crown DB",
        "status": "running",
    }


# =========================================================
# TREES
# =========================================================

@app.get("/trees")
def get_trees():

    with get_connection(DB_PATH) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT DISTINCT tree_id
            FROM crown_levels
            ORDER BY tree_id
            """
        )

        rows = cur.fetchall()

    return {
        "trees": [r["tree_id"] for r in rows]
    }


# =========================================================
# TREE LEVELS
# =========================================================

@app.get("/trees/{tree_id}/levels")
def get_tree_levels(tree_id: str):

    with get_connection(DB_PATH) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                h_level,
                data_type,
                roi_norm_path,
                roi_mask_norm_path,
                synth_method,
                synth_src_h
            FROM crown_levels
            WHERE tree_id = ?
            ORDER BY h_level
            """,
            (tree_id,),
        )

        rows = cur.fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"tree_id not found: {tree_id}"
        )

    result = []

    for r in rows:
        result.append({
            "h_level": r["h_level"],
            "data_type": r["data_type"],
            "roi_norm_path": r["roi_norm_path"],
            "roi_mask_norm_path": r["roi_mask_norm_path"],
            "synth_method": r["synth_method"],
            "synth_src_h": r["synth_src_h"],
        })

    return {
        "tree_id": tree_id,
        "levels": result,
    }


# =========================================================
# TREE STATUS
# =========================================================

@app.get("/trees/{tree_id}/status")
def get_tree_status(tree_id: str):

    with get_connection(DB_PATH) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                data_type,
                roi_norm_path,
                roi_mask_norm_path
            FROM crown_levels
            WHERE tree_id = ?
            """,
            (tree_id,),
        )

        rows = cur.fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"tree_id not found: {tree_id}"
        )

    total = len(rows)

    real_count = 0
    synth_count = 0
    roi_count = 0
    mask_count = 0

    for r in rows:

        dt = str(r["data_type"]).upper()

        if dt == "REAL":
            real_count += 1

        if dt == "SYNTH":
            synth_count += 1

        if r["roi_norm_path"]:
            roi_count += 1

        if r["roi_mask_norm_path"]:
            mask_count += 1

    return {
        "tree_id": tree_id,
        "total_levels": total,
        "real_levels": real_count,
        "synth_levels": synth_count,
        "levels_with_roi": roi_count,
        "levels_with_masks": mask_count,
    }


# =========================================================
# PREVIEW IMAGE
# =========================================================

@app.get("/trees/{tree_id}/preview")
def get_preview(tree_id: str):

    preview_path = (
        Path("data/tree_profiles")
        / tree_id
        / "preview.png"
    )

    if not preview_path.exists():
        raise HTTPException(
            status_code=404,
            detail="preview not found"
        )

    return FileResponse(preview_path)