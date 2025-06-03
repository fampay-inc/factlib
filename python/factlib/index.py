from datetime import datetime, timezone
from typing import Dict, Optional
from django.db import connection
from django.conf import settings
from factlib.pb.outbox_pb2 import OutboxEvent, TraceInfo
import sentry_sdk

class PostgresFactMixin:
    def __init__(self, event, prefix):
        """
        Initialize the PostgresFactMixin with an event and optional prefix
        """
        self._event = event
        self._prefix = prefix
    def emit(self):
        """
        Emit this event using pg_logical_emit_message
        """
        cursor = connection.cursor()
        try:
            sql_query = "SELECT pg_logical_emit_message(true, %s, %s::bytea)"
            cursor.execute(sql_query, (self._prefix, self._event.SerializeToString()))
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


class Fact(PostgresFactMixin):
    def __init__(
        self,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: bytes,
        metadata: Optional[Dict[str, str]] = None,
        prefix: str = None,
    ):
        # Ensure metadata is a dictionary of string keys and string values
        processed_metadata = {}
        trace_info = self._get_trace_context()
        if metadata:
            for key, value in metadata.items():
                # Convert keys and values to strings
                str_key = str(key)
                str_value = str(value) if value is not None else ""
                processed_metadata[str_key] = str_value

        self._event = OutboxEvent(
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            created_at=int(datetime.now(timezone.utc).timestamp() * 1_000_000),  # Convert to microseconds
            metadata=processed_metadata,
            trace_info=trace_info,
        )
        prefix = prefix or settings.FACTLIB_PREFIX
        super().__init__(self._event, prefix)

    def _get_trace_context(self) -> TraceInfo:
        span = sentry_sdk.Hub.current.scope.span
        if span is None:
            return {}
        return TraceInfo(
            trace_id=span.trace_id,
            span_id=span.span_id,
            metadata={"parent_op": span.op or ""}
        )
