import logging
import re
from pathlib import Path
from typing import Optional

from app.db.connection import get_connection


def _parse_altitude_from_filename(path_str: str) -> Optional[float]:
    """
    Ищет в имени файла высоту вида:
      '8м', '16 м', '12.5м', '12,5м'
    Возвращает число в метрах или None.
    """
    name = Path(path_str).name

    # ищем число + опциональные пробелы + "м"
    # поддерживаем 12.5 и 12,5
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*м", name, flags=re.IGNORECASE)
    if not m:
        return None

    s = m.group(1).replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def fill_flight_altitude_from_filename(db_path: Path) -> int:
    """
    Обновляет images.flight_altitude, беря высоту из имени файла path.
    Возвращает количество обновлённых строк.
    """
    updated = 0

    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute("SELECT image_id, path, flight_altitude FROM images")
        rows = cur.fetchall()

        for r in rows:
            image_id = r["image_id"]
            path_str = r["path"]
            current = r["flight_altitude"]

            alt = _parse_altitude_from_filename(path_str)
            if alt is None:
                continue

            # Обновляем, если поле пустое или отличается
            if current is None or float(current) != float(alt):
                cur.execute(
                    "UPDATE images SET flight_altitude = ? WHERE image_id = ?",
                    (alt, image_id),
                )
                updated += 1
                logging.info("Set flight_altitude=%.2f for %s", alt, path_str)

        conn.commit()

    logging.info("fill_flight_altitude_from_filename: updated=%d", updated)
    return updated
