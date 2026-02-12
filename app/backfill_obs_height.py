import logging
from pathlib import Path
from app.db.connection import get_connection


def backfill_obs_height_from_images(db_path: Path) -> int:
    """
    Заполняет obs_height в crown_observations из images.flight_altitude
    для тех записей, где obs_height IS NULL.

    Возвращает количество обновлённых строк.
    """
    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE crown_observations
            SET obs_height = (
                SELECT i.flight_altitude
                FROM images i
                WHERE i.image_id = crown_observations.image_id
            )
            WHERE obs_height IS NULL
            """
        )
        affected = cur.rowcount
        conn.commit()

    logging.info("Backfill obs_height done. Updated rows: %d", affected)
    return affected
