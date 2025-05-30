from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class OutboxEvent(_message.Message):
    __slots__ = ["id", "aggregate_type", "aggregate_id", "event_type", "payload", "created_at", "metadata", "trace_info"]
    class MetadataEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class TraceInfoEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_TYPE_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    TRACE_INFO_FIELD_NUMBER: _ClassVar[int]
    id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: bytes
    created_at: int
    metadata: _containers.ScalarMap[str, str]
    trace_info: _containers.ScalarMap[str, str]
    def __init__(self, id: _Optional[str] = ..., aggregate_type: _Optional[str] = ..., aggregate_id: _Optional[str] = ..., event_type: _Optional[str] = ..., payload: _Optional[bytes] = ..., created_at: _Optional[int] = ..., metadata: _Optional[_Mapping[str, str]] = ..., trace_info: _Optional[_Mapping[str, str]] = ...) -> None: ...
