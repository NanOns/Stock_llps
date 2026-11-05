"""在同一连接上取得短只读事务的PostgreSQL可见性快照。"""

from __future__ import annotations

from typing import Any, Mapping

from stock_a.storage.postgres.source_raw import (
    _connection_parameters,
    _validate_connection,
)


class SnapshotWitness:
    """一条不自动重连的连接；断开后不可再用于证明原连接身份。"""

    def __init__(self, config: Mapping[str, Any], expected_namespace: str) -> None:
        import psycopg

        parameters = _connection_parameters()
        parameters.update(
            connect_timeout=5,
            options="-c search_path=stock,pg_catalog -c statement_timeout=10000 -c lock_timeout=1000",
            autocommit=True,
        )
        self._connection = psycopg.connect(**parameters)
        self._config = config
        self._namespace = expected_namespace

    def sample(self) -> str:
        """在固定项目身份的短RR只读事务内取得完整64位snapshot。"""
        with self._connection.transaction():
            with self._connection.cursor() as cursor:
                cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                actual = _validate_connection(cursor, self._connection, self._config)
                if actual != self._namespace:
                    raise ValueError("快照见证的DEV命名空间不匹配")
                cursor.execute("SELECT pg_current_snapshot()::text")
                row = cursor.fetchone()
                if not row or not isinstance(row[0], str) or not row[0]:
                    raise ValueError("快照见证未返回有效的PG快照")
                return row[0]

    def close(self) -> None:
        self._connection.close()
