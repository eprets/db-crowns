import logging
from pathlib import Path
from app.db.connection import get_connection


def backfill_obs_height(db_path: Path) -> int:
    """
    Заполняет crown_observations.obs_height из images.flight_altitude
    по полю image_id, только там где obs_height IS NULL.
    Возвращает количество обновленных строк.
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
              AND image_id IN (
                SELECT image_id FROM images WHERE flight_altitude IS NOT NULL
              )
            """
        )

        updated = cur.rowcount
        conn.commit()

    logging.info("Backfill obs_height done. Updated rows: %d", updated)
    return updated
