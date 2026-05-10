# app/db/migrate_masks.py
import logging
from pathlib import Path

from app.db.connection import get_connection


def _has_column(db_path: Path, table: str, column: str) -> bool:
    """
    Проверяет, есть ли колонка column в таблице table.
    """
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table});")
        rows = cur.fetchall()

    columns = [r["name"] for r in rows]
    return column in columns


def _add_column_if_missing(db_path: Path, table: str, column: str, column_type: str) -> None:
    """
    Добавляет колонку, если её ещё нет.
    """
    if _has_column(db_path, table, column):
        logging.info("OK: column already exists: %s.%s", table, column)
        return

    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type};")
        conn.commit()

    logging.info("Added column: %s.%s (%s)", table, column, column_type)


def migrate_masks(db_path: Path) -> None:
    """
    Миграция БД для хранения масок.

    Добавляем:
    - annotations.mask_path
    - crown_observations.roi_mask_raw_path
    - crown_levels.roi_mask_norm_path
    """

    _add_column_if_missing(
        db_path=db_path,
        table="annotations",
        column="mask_path",
        column_type="TEXT",
    )

    _add_column_if_missing(
        db_path=db_path,
        table="crown_observations",
        column="roi_mask_raw_path",
        column_type="TEXT",
    )

    _add_column_if_missing(
        db_path=db_path,
        table="crown_levels",
        column="roi_mask_norm_path",
        column_type="TEXT",
    )