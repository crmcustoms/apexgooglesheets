"""
SQLite-хранилище для отслеживания уже выгруженных операций.
Предотвращает дубликаты при повторных запусках.
"""

import sqlite3
import logging
from pathlib import Path
from contextlib import contextmanager

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS synced_operations (
    operation_id    TEXT NOT NULL,
    operation_type  TEXT NOT NULL,   -- 'fact' или 'plan'
    year            INTEGER NOT NULL,
    sheet_row       INTEGER,         -- номер строки в таблице (опционально)
    synced_at       TEXT NOT NULL DEFAULT (datetime('now')),
    last_status     TEXT,            -- последний известный статус операции
    PRIMARY KEY (operation_id, operation_type)
);

CREATE INDEX IF NOT EXISTS idx_year ON synced_operations(year);
CREATE INDEX IF NOT EXISTS idx_type ON synced_operations(operation_type);
"""


class SyncDatabase:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript(CREATE_TABLE_SQL)
        logger.info("База данных инициализирована: %s", self.db_path)

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def is_synced(self, operation_id: str, operation_type: str) -> bool:
        """Возвращает True, если операция уже была выгружена."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM synced_operations WHERE operation_id=? AND operation_type=?",
                (str(operation_id), operation_type),
            ).fetchone()
            return row is not None

    def get_status(self, operation_id: str, operation_type: str) -> str | None:
        """Возвращает последний известный статус операции."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_status FROM synced_operations WHERE operation_id=? AND operation_type=?",
                (str(operation_id), operation_type),
            ).fetchone()
            return row["last_status"] if row else None

    def mark_synced(
        self,
        operation_id: str,
        operation_type: str,
        year: int,
        last_status: str | None = None,
        sheet_row: int | None = None,
    ):
        """Записывает операцию как успешно выгруженную."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO synced_operations
                    (operation_id, operation_type, year, last_status, sheet_row, synced_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(operation_id, operation_type) DO UPDATE SET
                    last_status = excluded.last_status,
                    sheet_row   = excluded.sheet_row,
                    synced_at   = excluded.synced_at
                """,
                (str(operation_id), operation_type, year, last_status, sheet_row),
            )

    def mark_synced_batch(self, records: list[dict]):
        """
        Пакетная запись.
        records: список словарей с ключами:
            operation_id, operation_type, year, last_status (opt), sheet_row (opt)
        """
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO synced_operations
                    (operation_id, operation_type, year, last_status, sheet_row, synced_at)
                VALUES (:operation_id, :operation_type, :year, :last_status, :sheet_row, datetime('now'))
                ON CONFLICT(operation_id, operation_type) DO UPDATE SET
                    last_status = excluded.last_status,
                    sheet_row   = excluded.sheet_row,
                    synced_at   = excluded.synced_at
                """,
                [
                    {
                        "operation_id": str(r["operation_id"]),
                        "operation_type": r["operation_type"],
                        "year": r["year"],
                        "last_status": r.get("last_status"),
                        "sheet_row": r.get("sheet_row"),
                    }
                    for r in records
                ],
            )

    def get_all_synced_ids(self, operation_type: str) -> set[str]:
        """Возвращает множество всех ID выгруженных операций заданного типа."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT operation_id FROM synced_operations WHERE operation_type=?",
                (operation_type,),
            ).fetchall()
            return {row["operation_id"] for row in rows}

    def stats(self) -> dict:
        """Статистика по базе данных."""
        with self._connect() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM synced_operations"
            ).fetchone()[0]
            by_type = conn.execute(
                "SELECT operation_type, COUNT(*) as cnt FROM synced_operations GROUP BY operation_type"
            ).fetchall()
            by_year = conn.execute(
                "SELECT year, COUNT(*) as cnt FROM synced_operations GROUP BY year ORDER BY year"
            ).fetchall()
        return {
            "total": total,
            "plan": dict(by_type).get("plan", 0),
            "fact": dict(by_type).get("fact", 0),
            "by_type": {row["operation_type"]: row["cnt"] for row in by_type},
            "by_year": {row["year"]: row["cnt"] for row in by_year},
        }

    def clear_year(self, year: int, operation_type: str):
        """Удаляет все записи за год и тип (используется при --force)."""
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM synced_operations WHERE year=? AND operation_type=?",
                (year, operation_type),
            )

    def close(self):
        """Закрывает соединение (совместимость)."""
        pass
