from pathlib import Path
from app.db.connection import get_connection


def cleanup_orphan_observations(db_path: Path) -> int:
    """
    Удаляет observations, у которых annotation_id больше не существует (orphan records).
    Возвращает количество удалённых строк.
    """
    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT o.obs_id
            FROM crown_observations o
            LEFT JOIN annotations a ON a.annotation_id = o.annotation_id
            WHERE a.annotation_id IS NULL
            """
        )
        to_delete = [r[0] for r in cur.fetchall()]

        for obs_id in to_delete:
            cur.execute("DELETE FROM crown_observations WHERE obs_id = ?", (obs_id,))

        conn.commit()
        return len(to_delete)
