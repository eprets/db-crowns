import json
from pathlib import Path
from typing import List, Dict, Any

from app.db.connection import get_connection


def list_images(db_path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT image_id, path, created_at
            FROM images
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [dict(r) for r in rows]


def count_images(db_path: Path) -> int:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM images")
        row = cur.fetchone()
    return int(row["cnt"])


def count_annotations(db_path: Path) -> int:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM annotations")
        row = cur.fetchone()
    return int(row["cnt"])


def list_annotations(db_path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                a.annotation_id,
                a.tree_id,
                a.tree_type,
                a.image_id,
                i.path,
                a.x0, a.y0, a.a, a.b, a.theta,
                a.created_at
            FROM annotations a
            JOIN images i ON i.image_id = a.image_id
            ORDER BY a.created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [dict(r) for r in rows]

def count_observations(db_path: Path) -> int:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS cnt FROM crown_observations")
        row = cur.fetchone()
    return int(row["cnt"])


def list_observations(db_path: Path, limit: int = 20) -> List[Dict[str, Any]]:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                o.obs_id,
                o.tree_id,
                o.image_id,
                i.path AS image_path,
                o.roi_raw_path,
                o.features_json,
                o.created_at
            FROM crown_observations o
            JOIN images i ON i.image_id = o.image_id
            ORDER BY o.created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    for r in rows:
        try:
            r["features"] = json.loads(r["features_json"]) if r["features_json"] else {}
        except Exception as e:
            r["features"] = {}
            r["features_parse_error"] = str(e)

    return rows