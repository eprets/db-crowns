# app/db/migrate_crown_levels_pix2pix.py
import logging
from pathlib import Path

from app.db.connection import get_connection


def _has_column(db_path: Path, table: str, column: str) -> bool:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table});")
        cols = [r["name"] for r in cur.fetchall()]
    return column in cols


def migrate_crown_levels_for_pix2pix(db_path: Path) -> None:
    """
    Добавляет в crown_levels колонки для GAN-синтеза:
    - synth_method TEXT
    - synth_src_h REAL
    Безопасно: если колонка уже есть — пропускает.
    """
    table = "crown_levels"

    to_add = [
        ("synth_method", "TEXT"),
        ("synth_src_h", "REAL"),
    ]

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        for col, coltype in to_add:
            if _has_column(db_path, table, col):
                logging.info("OK: column already exists: %s.%s", table, col)
                continue

            sql = f"ALTER TABLE {table} ADD COLUMN {col} {coltype};"
            cur.execute(sql)
            logging.info("Added column: %s.%s (%s)", table, col, coltype)

        conn.commit()