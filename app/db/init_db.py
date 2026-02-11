import sqlite3
from pathlib import Path


def init_db(db_path: Path, schema_path: Path) -> None:
    """
    Создаёт SQLite базу и таблицы по schema.sql.
    Важно: выполняем по statement'ам и НЕ падаем, если один statement (например UNIQUE INDEX)
    не применился из-за существующих дублей.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        with schema_path.open("r", encoding="utf-8") as f:
            schema_sql = f.read()

        # Разбиваем на отдельные команды по ';'
        statements = [s.strip() for s in schema_sql.split(";") if s.strip()]

        cur = conn.cursor()
        for stmt in statements:
            try:
                cur.execute(stmt)
            except sqlite3.IntegrityError as e:
                # чаще всего тут падает UNIQUE INDEX, если в данных есть дубли
                print(f"[WARN] Skipped statement due to IntegrityError: {e}\n  -> {stmt[:120]}...")
            except sqlite3.OperationalError as e:
                print(f"[WARN] Skipped statement due to OperationalError: {e}\n  -> {stmt[:120]}...")

        conn.commit()
    finally:
        conn.close()
