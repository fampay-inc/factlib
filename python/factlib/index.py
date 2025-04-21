import time
from django.db import connection
from django.conf import settings
from factlib.pb.outbox_pb2 import OutboxEvent


class PostgresFactMixin:
    def emit(self):
        """
        Emit this event using pg_logical_emit_message
        """
        cursor = connection.cursor()
        try:
            sql_query = "SELECT pg_logical_emit_message(true, %s, %s::bytea)"
            cursor.execute(sql_query, (self.prefix, self.SerializeToString()))
        except Exception as e:
            raise RuntimeError("Failed to emit event") from e
        finally:
            cursor.close()

    def with_prefix(self, prefix: str):
        """
        Set the prefix for emission
        """
        self.prefix = prefix
        return self


class Fact(PostgresFactMixin, OutboxEvent):
    def __init__(
        self,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: bytes,
        metadata: dict[str, str],
    ):
        super().__init__(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            created_at=int(time.time()),
            metadata=metadata,
        )
        self.prefix = settings.FACT_PREFIX
