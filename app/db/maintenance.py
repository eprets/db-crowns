from pathlib import Path
from app.db.connection import get_connection


def deduplicate_annotations_keep_latest(db_path: Path) -> int:
    """
    Удаляет дубли аннотаций по (image_id, tree_id), оставляет только самую новую (по created_at).
    Возвращает количество удалённых строк.
    """
    with get_connection(db_path) as conn:
        cur = conn.cursor()

        # Найдём все annotation_id, которые надо удалить (все кроме самой новой)
        cur.execute(
            """
            SELECT a.annotation_id
            FROM annotations a
            JOIN (
                SELECT image_id, tree_id, MAX(created_at) AS max_created
                FROM annotations
                GROUP BY image_id, tree_id
            ) latest
            ON a.image_id = latest.image_id AND a.tree_id = latest.tree_id
            WHERE a.created_at < latest.max_created
            """
        )
        to_delete = [r[0] for r in cur.fetchall()]

        for ann_id in to_delete:
            cur.execute("DELETE FROM annotations WHERE annotation_id = ?", (ann_id,))

        conn.commit()
        return len(to_delete)
