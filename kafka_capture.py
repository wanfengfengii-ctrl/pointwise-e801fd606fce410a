"""Read-committed extractor for captured Kafka magic=2 record batches.

The Reader consumes a single partition's consecutive fetched-batch byte stream
in arbitrary slices and yields only visible user records.  A batch is validated
completely (framing, CRC32C, decompression and every record) before any record
from it is emitted.  Any wire error raises DecodeError and permanently fails
the reader.
"""

import struct
import zlib

__all__ = ["DecodeError", "Reader"]

_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1

_MAGIC2_HEADER_SIZE = 61
_MIN_BATCH_LENGTH = 49


class DecodeError(Exception):
    """Invalid, unsupported or truncated Kafka record-batch bytes."""


def _build_crc32c_table():
    table = []
    for value in range(256):
        crc = value
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if crc & 1 else crc >> 1
        table.append(crc)
    return tuple(table)


_CRC32C_TABLE = _build_crc32c_table()


def crc32c(data):
    """CRC32C (Castagnoli) of a bytes-like object."""
    crc = 0xFFFFFFFF
    table = _CRC32C_TABLE
    for octet in data:
        crc = table[(crc ^ octet) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def _decode_varint(data, pos, end, max_octets, bits, label):
    """Decode one zig-zag LEB128 value bounded by ``end``."""
    unsigned = 0
    shift = 0
    for _ in range(max_octets):
        if pos >= end:
            raise DecodeError("truncated %s" % label)
        octet = data[pos]
        pos += 1
        unsigned |= (octet & 0x7F) << shift
        if not octet & 0x80:
            if unsigned >= (1 << bits):
                raise DecodeError("%s overflows signed %d bits" % (label, bits))
            return (unsigned >> 1) ^ -(unsigned & 1), pos
        shift += 7
    raise DecodeError("%s exceeds %d octets" % (label, max_octets))


def _varint(data, pos, end, label="varint"):
    return _decode_varint(data, pos, end, 5, 32, label)


def _varlong(data, pos, end, label="varlong"):
    return _decode_varint(data, pos, end, 10, 64, label)


def _take(data, pos, end, size, label):
    if size < 0 or size > end - pos:
        raise DecodeError("truncated %s" % label)
    return bytes(data[pos:pos + size]), pos + size


def _nullable_bytes(data, pos, end, label):
    size, pos = _varint(data, pos, end, label + " length")
    if size == -1:
        return None, pos
    if size < -1:
        raise DecodeError("invalid %s length %d" % (label, size))
    return _take(data, pos, end, size, label)


def _gunzip(data):
    """Decode all concatenated gzip members, rejecting trailing garbage."""
    remaining = data
    parts = []
    while True:
        if remaining[:2] != b"\x1f\x8b":
            raise DecodeError("malformed gzip stream")
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            chunk = decompressor.decompress(remaining)
            chunk += decompressor.flush()
        except zlib.error as exc:
            raise DecodeError("malformed gzip stream: %s" % exc)
        if not decompressor.eof:
            raise DecodeError("incomplete gzip member")
        parts.append(chunk)
        remaining = decompressor.unused_data
        if not remaining:
            return b"".join(parts)


class _Record:
    __slots__ = ("offset", "timestamp", "key", "value", "headers")

    def __init__(self, offset, timestamp, key, value, headers):
        self.offset = offset
        self.timestamp = timestamp
        self.key = key
        self.value = value
        self.headers = headers


class Reader:
    """Incremental read_committed extractor.

    ``aborted`` lists ``{"producer_id", "first_offset"}`` transactions aborted
    in this fetch interval.  ``feed`` accepts arbitrary byte slices and returns
    the visible user records completed so far; ``finish`` seals the stream.
    """

    def __init__(self, start_offset, last_stable_offset, aborted):
        self._start_offset = start_offset
        self._last_stable_offset = last_stable_offset
        # Only visibility metadata is retained between calls.
        self._pending = sorted(
            ((entry["producer_id"], entry["first_offset"]) for entry in aborted),
            key=lambda entry: entry[1],
        )
        self._next_pending = 0
        self._active = {}
        self._buffer = bytearray()
        self._previous_end = None
        self._finished = False
        self._failure = None

    # -- public API -----------------------------------------------------

    def feed(self, data):
        self._ensure_live()
        if self._finished:
            self._failure = "feed after finish"
            raise DecodeError(self._failure)
        self._buffer.extend(data)
        emitted = []
        try:
            while True:
                if len(self._buffer) < 12:
                    break
                base_offset, batch_length = struct.unpack_from(">qi", self._buffer, 0)
                if batch_length < _MIN_BATCH_LENGTH:
                    raise DecodeError(
                        "invalid batchLength %d" % batch_length)
                total = 12 + batch_length
                if len(self._buffer) < total:
                    break
                batch = bytes(self._buffer[:total])
                del self._buffer[:total]
                emitted.extend(self._process_batch(batch, base_offset, batch_length))
        except DecodeError as exc:
            self._buffer.clear()
            self._failure = str(exc)
            raise
        return emitted

    def finish(self):
        self._ensure_live()
        if self._finished:
            return None
        if self._buffer:
            self._buffer.clear()
            self._failure = "truncated batch at end of input"
            raise DecodeError(self._failure)
        self._finished = True
        return None

    # -- internal -------------------------------------------------------

    def _ensure_live(self):
        if self._failure is not None:
            raise DecodeError("reader permanently failed: %s" % self._failure)

    def _activate_aborted(self, base_offset):
        pending = self._pending
        while self._next_pending < len(pending):
            producer_id, first_offset = pending[self._next_pending]
            if first_offset > base_offset:
                break
            self._active[producer_id] = first_offset
            self._next_pending += 1

    def _process_batch(self, batch, base_offset, batch_length):
        if batch[16] != 2:
            raise DecodeError("unsupported magic %d" % batch[16])

        (attributes, last_offset_delta, first_timestamp, max_timestamp,
         producer_id, _producer_epoch, _base_sequence,
         records_count) = struct.unpack_from(">hiqqqhii", batch, 21)

        if base_offset < 0:
            raise DecodeError("negative baseOffset %d" % base_offset)
        if last_offset_delta < 0:
            raise DecodeError("negative lastOffsetDelta %d" % last_offset_delta)
        if records_count < 0:
            raise DecodeError("negative recordsCount %d" % records_count)
        if base_offset + last_offset_delta > _INT64_MAX:
            raise DecodeError("batch offset range overflows signed 64 bits")

        stored_crc = struct.unpack_from(">I", batch, 17)[0]
        body = batch[21:]
        if crc32c(body) != stored_crc:
            raise DecodeError("CRC32C mismatch")

        codec = attributes & 0x07
        if codec == 0:
            record_area = batch[_MAGIC2_HEADER_SIZE:]
        elif codec == 1:
            record_area = _gunzip(batch[_MAGIC2_HEADER_SIZE:])
        else:
            raise DecodeError("unsupported compression codec %d" % codec)

        records = self._parse_records(
            record_area, records_count, base_offset, last_offset_delta,
            first_timestamp, max_timestamp, bool(attributes & 0x08))

        if self._previous_end is not None and base_offset <= self._previous_end:
            raise DecodeError("overlapping batch at offset %d" % base_offset)
        self._previous_end = base_offset + last_offset_delta

        # Visibility metadata is updated for every batch, including batches
        # outside the requested range or hidden from output.
        self._activate_aborted(base_offset)

        transactional = bool(attributes & 0x10)
        is_control = bool(attributes & 0x20)

        if is_control:
            self._validate_control(
                codec, records, base_offset, producer_id, transactional)
            return []

        if transactional and producer_id in self._active:
            return []

        emitted = []
        start = self._start_offset
        stop = self._last_stable_offset
        for record in records:
            if start <= record.offset < stop:
                emitted.append({
                    "offset": record.offset,
                    "timestamp": record.timestamp,
                    "key": record.key,
                    "value": record.value,
                    "headers": record.headers,
                })
        return emitted

    def _parse_records(self, data, count, base_offset, last_offset_delta,
                       first_timestamp, max_timestamp, append_time):
        area_end = len(data)
        pos = 0
        previous_delta = None
        records = []
        for _ in range(count):
            length, pos = _varint(data, pos, area_end, "record length")
            if length < 0:
                raise DecodeError("negative record length %d" % length)
            record_end = pos + length
            if record_end > area_end:
                raise DecodeError("record extends past record area")

            cursor = pos
            if cursor >= record_end:
                raise DecodeError("truncated record")
            cursor += 1  # record attributes are accepted but unused
            timestamp_delta, cursor = _varlong(
                data, cursor, record_end, "timestamp delta")
            offset_delta, cursor = _varint(
                data, cursor, record_end, "offset delta")
            key, cursor = _nullable_bytes(data, cursor, record_end, "key")
            value, cursor = _nullable_bytes(data, cursor, record_end, "value")
            header_count, cursor = _varint(
                data, cursor, record_end, "headers count")
            if header_count < 0:
                raise DecodeError("negative headers count %d" % header_count)
            headers = []
            for _ in range(header_count):
                key_length, cursor = _varint(
                    data, cursor, record_end, "header key length")
                if key_length < 0:
                    raise DecodeError("header key cannot be null")
                header_key, cursor = _take(
                    data, cursor, record_end, key_length, "header key")
                header_value, cursor = _nullable_bytes(
                    data, cursor, record_end, "header value")
                headers.append((header_key, header_value))

            if cursor != record_end:
                raise DecodeError("record length does not match its contents")
            pos = record_end

            if offset_delta < 0:
                raise DecodeError("negative offset delta %d" % offset_delta)
            if previous_delta is not None and offset_delta <= previous_delta:
                raise DecodeError("offset deltas are not strictly increasing")
            if offset_delta > last_offset_delta:
                raise DecodeError("offset delta exceeds lastOffsetDelta")
            previous_delta = offset_delta

            if append_time:
                timestamp = max_timestamp
            else:
                timestamp = first_timestamp + timestamp_delta
                if not _INT64_MIN <= timestamp <= _INT64_MAX:
                    raise DecodeError("timestamp overflows signed 64 bits")

            offset = base_offset + offset_delta
            if offset < 0 or offset > _INT64_MAX:
                raise DecodeError("offset overflows signed 64 bits")

            records.append(_Record(offset, timestamp, key, value, headers))

        if pos != area_end:
            raise DecodeError("record area is not fully consumed")
        return records

    def _validate_control(self, codec, records, base_offset,
                          producer_id, transactional):
        if not transactional:
            raise DecodeError("control batch is not transactional")
        if codec != 0:
            raise DecodeError("control batch is compressed")
        if len(records) != 1:
            raise DecodeError("control batch must contain exactly one record")
        record = records[0]
        if record.offset != base_offset:
            raise DecodeError("control record is not at delta zero")
        if record.key is None or len(record.key) != 4:
            raise DecodeError("malformed control record key")
        version, control_type = struct.unpack(">hh", record.key)
        if version != 0 or control_type not in (0, 1):
            raise DecodeError("malformed control record key")
        if record.value is None or len(record.value) != 6:
            raise DecodeError("malformed control record value")
        value_version, _coordinator_epoch = struct.unpack(">hi", record.value)
        if value_version != 0:
            raise DecodeError("malformed control record value")
        if control_type == 0:  # ABORT terminates this producer's interval
            self._active.pop(producer_id, None)
