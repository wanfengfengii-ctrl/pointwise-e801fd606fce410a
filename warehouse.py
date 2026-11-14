"""Business view over read_committed records: shape checks and row encoding.

The physical payload bytes are never transcoded.  A key, value or header value
that is present but empty stays distinguishable from an absent (``None``) one:
rows encode bytes as lowercase hex, with ``None`` kept as JSON null.
"""

__all__ = ["RecordShapeError", "validate_record", "build_rows"]

_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1
_FIELDS = {"offset", "timestamp", "key", "value", "headers"}


class RecordShapeError(ValueError):
    """A record does not satisfy the reader's output contract."""


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def validate_record(record):
    if type(record) is not dict or set(record) != _FIELDS:
        raise RecordShapeError("record must have exactly %r" % sorted(_FIELDS))
    offset = record["offset"]
    timestamp = record["timestamp"]
    if not _is_int(offset) or offset < 0:
        raise RecordShapeError("offset must be a nonnegative int")
    if not _is_int(timestamp) or not _INT64_MIN <= timestamp <= _INT64_MAX:
        raise RecordShapeError("timestamp must be a signed 64-bit int")
    for field in ("key", "value"):
        payload = record[field]
        if payload is not None and type(payload) is not bytes:
            raise RecordShapeError("%s must be bytes or None" % field)
    headers = record["headers"]
    if type(headers) is not list:
        raise RecordShapeError("headers must be a list")
    for header in headers:
        if type(header) is not tuple or len(header) != 2:
            raise RecordShapeError("header must be a (key, value) tuple")
        header_key, header_value = header
        if type(header_key) is not bytes:
            raise RecordShapeError("header key must be bytes and non-null")
        if header_value is not None and type(header_value) is not bytes:
            raise RecordShapeError("header value must be bytes or None")
    return record


def _hex(payload):
    return None if payload is None else payload.hex()


def build_rows(records, partition=0):
    """Validate and project visible records into deterministic row dicts."""
    if not _is_int(partition) or partition < 0:
        raise RecordShapeError("partition must be a nonnegative int")
    rows = []
    previous = None
    for record in records:
        validate_record(record)
        offset = record["offset"]
        if previous is not None and offset <= previous:
            raise RecordShapeError("visible records are not in increasing offset order")
        previous = offset
        rows.append({
            "partition": partition,
            "offset": offset,
            "timestamp": record["timestamp"],
            "key": _hex(record["key"]),
            "value": _hex(record["value"]),
            "headers": [[header_key.hex(), _hex(header_value)]
                        for header_key, header_value in record["headers"]],
        })
    return rows
