from pathlib import Path
from app.db.connection import get_connection


def print_heights_summary(db_path: Path, limit: int = 20) -> None:
    with get_connection(db_path) as conn:
        cur = conn.cursor()

        print("\n--- IMAGES (flight_altitude) ---")
        cur.execute(
            """
            SELECT image_id, path, flight_altitude
            FROM images
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        for r in rows:
            print(f"- image_id={r['image_id']}  flight_altitude={r['flight_altitude']}  path={r['path']}")

        print("\n--- OBSERVATIONS (obs_height) ---")
        cur.execute(
            """
            SELECT o.obs_id, o.tree_id, o.image_id, o.obs_height, i.flight_altitude
            FROM crown_observations o
            JOIN images i ON i.image_id = o.image_id
            ORDER BY o.created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        for r in rows:
            print(
                f"- obs_id={r['obs_id']}  tree_id={r['tree_id']}  "
                f"obs_height={r['obs_height']}  flight_altitude={r['flight_altitude']}"
            )
