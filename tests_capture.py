"""Contract checks for kafka_capture.Reader and the warehouse projection."""

import gzip
import struct
import unittest

import warehouse
import writer
from kafka_capture import DecodeError, Reader, crc32c

INT64_MAX = (1 << 63) - 1
INT64_MIN = -(1 << 63)
START_OFFSET = 40


def assemble(base, area, count, attributes=0, last=0,
             first_ts=1000, max_ts=None, producer=-1, epoch=-1, seq=0):
    """Build one physical magic=2 batch around an encoded record area."""
    if max_ts is None:
        max_ts = first_ts
    body = struct.pack(">hiqqqhii", attributes, last, first_ts, max_ts,
                       producer, epoch, seq, count) + area
    crc = crc32c(body)
    tail = struct.pack(">ibI", 0, 2, crc) + body
    return struct.pack(">qi", base, len(tail)) + tail


def area(*records):
    return b"".join(records)


def rec(delta, value=b"v", key=None, headers=(), timestamp=0):
    return writer.record(delta, value, key=key, headers=headers,
                         timestamp=timestamp)


def control_record(control_type=0, key_version=0, value_version=0,
                   key=None, value=None):
    if key is None:
        key = struct.pack(">hh", key_version, control_type)
    if value is None:
        value = struct.pack(">hi", value_version, 0)
    return rec(0, key=key, value=value)


def extract(raw, start=0, lso=1000, aborted=(), chunk=None):
    reader = Reader(start, lso, aborted)
    out = []
    size = len(raw) if chunk is None else chunk
    for begin in range(0, len(raw), max(size, 1)):
        out.extend(reader.feed(raw[begin:begin + size]))
    reader.finish()
    return out


def offsets(records):
    return [record["offset"] for record in records]


class CaptureExpected(unittest.TestCase):

    def test_capture_visible_result_matches_contract(self):
        raw = writer.capture()
        expected = [
            {"offset": 41, "timestamp": 996, "key": None,
             "value": b"keep-B",
             "headers": [(b"x", None), (b"x", b"2")]},
            {"offset": 43, "timestamp": 1000, "key": None,
             "value": None, "headers": []},
        ]
        self.assertEqual(
            extract(raw, START_OFFSET, writer.LAST_STABLE,
                    writer.ABORTED, 7), expected)

    def test_every_slice_size_agrees(self):
        raw = writer.capture()
        baseline = extract(raw, START_OFFSET, writer.LAST_STABLE,
                           writer.ABORTED)
        for size in range(1, len(raw) + 2):
            self.assertEqual(
                extract(raw, START_OFFSET, writer.LAST_STABLE,
                        writer.ABORTED, size), baseline, "slice %d" % size)

    def test_warehouse_rows_keep_null_and_empty_distinct(self):
        rows = warehouse.build_rows(
            extract(writer.capture(), START_OFFSET,
                    writer.LAST_STABLE, writer.ABORTED))
        self.assertEqual([row["offset"] for row in rows], [41, 43])
        self.assertIsNone(rows[0]["key"])
        self.assertEqual(rows[0]["value"], b"keep-B".hex())
        self.assertEqual(rows[0]["headers"], [[b"x".hex(), None],
                                              [b"x".hex(), b"2".hex()]])
        self.assertIsNone(rows[1]["value"])
        self.assertEqual(rows[1]["headers"], [])


class Visibility(unittest.TestCase):

    def test_aborted_interval_hides_only_until_marker(self):
        raw = (assemble(10, area(rec(0)), 1, 16, 0, producer=5)
               + assemble(11, area(control_record(0)), 1, 16 | 32, 0,
                          producer=5)
               + assemble(12, area(rec(0)), 1, 16, 0, producer=5))
        self.assertEqual(offsets(extract(raw, 0, 100,
                                         [{"producer_id": 5,
                                           "first_offset": 10}])), [12])

    def test_fetch_starting_inside_aborted_transaction_hides_batch(self):
        raw = assemble(12, area(rec(0)), 1, 16, 0, producer=5)
        self.assertEqual(extract(raw, 12, 100,
                                 [{"producer_id": 5,
                                   "first_offset": 10}]), [])

    def test_aborted_entry_before_start_offset_activates(self):
        raw = assemble(12, area(rec(0)), 1, 16, 0, producer=5)
        self.assertEqual(extract(raw, 12, 100, []), [
            {"offset": 12, "timestamp": 1000, "key": None,
             "value": b"v", "headers": []}])
        self.assertEqual(extract(raw, 12, 100,
                                 [{"producer_id": 5,
                                   "first_offset": 3}]), [])

    def test_nontransactional_batch_is_not_hidden(self):
        raw = assemble(1, area(rec(0)), 1, 0, 0, producer=5)
        self.assertEqual(offsets(extract(raw, 0, 100,
                                         [{"producer_id": 5,
                                           "first_offset": 0}])), [1])

    def test_commit_marker_does_not_create_aborted_interval(self):
        raw = (assemble(1, area(control_record(1)), 1, 16 | 32, 0,
                        producer=5)
               + assemble(2, area(rec(0)), 1, 16, 0, producer=5))
        self.assertEqual(offsets(extract(raw, 0, 100,
                                         [{"producer_id": 5,
                                           "first_offset": 0}])), [])
        self.assertEqual(offsets(extract(raw, 0, 100, [])), [2])

    def test_marker_for_unlisted_transaction_is_legal(self):
        raw = assemble(3, area(control_record(0)), 1, 16 | 32, 0, producer=9)
        self.assertEqual(extract(raw, 0, 100, []), [])

    def test_marker_after_lso_still_updates_state(self):
        raw = (assemble(0, area(rec(0)), 1, 16, 0, producer=7)
               + assemble(7, area(control_record(0)), 1, 16 | 32, 0,
                          producer=7)
               + assemble(8, area(rec(0)), 1, 16, 0, producer=7))
        self.assertEqual(offsets(extract(raw, 0, 5,
                                         [{"producer_id": 7,
                                           "first_offset": 0}])), [])

    def test_producer_epoch_is_not_used_for_matching(self):
        raw = (assemble(0, area(rec(0)), 1, 16, 0, producer=7, epoch=99)
               + assemble(1, area(rec(0)), 1, 16, 0, producer=8, epoch=1))
        self.assertEqual(offsets(extract(raw, 0, 100,
                                         [{"producer_id": 7,
                                           "first_offset": 0}])), [1])

    def test_same_producer_later_transaction_visible(self):
        raw = (assemble(20, area(rec(0)), 1, 16, 0, producer=3)
               + assemble(21, area(control_record(0)), 1, 16 | 32, 0,
                          producer=3)
               + assemble(22, area(rec(0)), 1, 16, 0, producer=3)
               + assemble(23, area(rec(0)), 1, 16, 0, producer=3))
        self.assertEqual(offsets(extract(raw, 20, 100,
                                         [{"producer_id": 3,
                                           "first_offset": 20}])), [22, 23])

    def test_out_of_range_records_are_filtered(self):
        raw = (assemble(4, area(rec(0)), 1, 0, 0)
               + assemble(5, area(rec(0)), 1, 0, 0)
               + assemble(6, area(rec(0)), 1, 0, 0))
        self.assertEqual(offsets(extract(raw, 5, 6)), [5])


class Framing(unittest.TestCase):

    def test_incomplete_batch_emits_nothing_until_complete(self):
        raw = assemble(0, area(rec(0)), 1, 0, 0)
        for cut in range(len(raw)):
            reader = Reader(0, 100, [])
            self.assertEqual(reader.feed(raw[:cut]), [], "cut %d" % cut)

    def test_incremental_feed_returns_only_complete_batches(self):
        first = assemble(0, area(rec(0)), 1, 0, 0)
        second = assemble(1, area(rec(0)), 1, 0, 0)
        reader = Reader(0, 100, [])
        out = reader.feed(first + second[:5])
        self.assertEqual(offsets(out), [0])
        self.assertEqual(offsets(reader.feed(second[5:])), [1])
        reader.finish()

    def test_output_is_in_original_offset_order(self):
        raw = b"".join(assemble(base, area(rec(0)), 1, 0, 0)
                       for base in (100, 102, 104))
        self.assertEqual(offsets(extract(raw, 100, 1000)), [100, 102, 104])

    def test_compaction_gaps_and_empty_batches(self):
        raw = (assemble(10, area(rec(0), rec(5)), 2, 0, 5)
               + assemble(16, b"", 0, 0, 3)
               + assemble(20, area(rec(0)), 1, 0, 0))
        self.assertEqual(offsets(extract(raw, 10, 1000)), [10, 15, 20])

    def test_append_time_uses_max_timestamp(self):
        raw = assemble(0, area(rec(0, timestamp=7)), 1, 8, 0,
                       first_ts=500, max_ts=1234)
        self.assertEqual(extract(raw, 0, 100)[0]["timestamp"], 1234)

    def test_log_append_bit_ignored_when_absent(self):
        raw = assemble(0, area(rec(0, timestamp=-3)), 1, 0, 0, first_ts=800)
        self.assertEqual(extract(raw, 0, 100)[0]["timestamp"], 797)

    def test_multi_member_gzip_is_accepted(self):
        payload = area(rec(0, value=b"first"), rec(1, value=b"second"))
        compressed = (gzip.compress(payload[:9], mtime=0)
                      + gzip.compress(payload[9:], mtime=0))
        raw = assemble(0, compressed, 2, 1, 1)
        self.assertEqual(offsets(extract(raw, 0, 100)), [0, 1])

    def test_empty_bytes_and_none_differ(self):
        raw = assemble(0, area(rec(0, value=b"", key=None),
                               rec(1, value=None, key=b"")), 2, 0, 1)
        records = extract(raw, 0, 100)
        self.assertEqual(records[0]["key"], None)
        self.assertEqual(records[0]["value"], b"")
        self.assertEqual(records[1]["key"], b"")
        self.assertEqual(records[1]["value"], None)
        rows = warehouse.build_rows(records)
        self.assertEqual((rows[0]["key"], rows[0]["value"]), (None, ""))
        self.assertEqual((rows[1]["key"], rows[1]["value"]), ("", None))

    def test_header_order_and_duplicate_keys_preserved(self):
        raw = assemble(0, area(rec(
            0, headers=[(b"b", b"1"), (b"a", None), (b"b", b"1")])), 1, 0, 0)
        headers = extract(raw, 0, 100)[0]["headers"]
        self.assertEqual(headers, [(b"b", b"1"), (b"a", None), (b"b", b"1")])

    def test_nonminimal_varint_is_accepted(self):
        body = (b"\x00" + b"\x00" + b"\x80\x00" + b"\x01"
                + b"\x02v" + b"\x00")
        raw = assemble(0, writer.vint(len(body)) + body, 1, 0, 0)
        records = extract(raw, 0, 100)
        self.assertEqual(offsets(records), [0])
        self.assertEqual(records[0]["value"], b"v")


class Rejections(unittest.TestCase):

    def assert_fails(self, raw, start=0, lso=1000, aborted=(), chunk=None):
        reader = Reader(start, lso, aborted)
        with self.assertRaises(DecodeError):
            if chunk is None:
                reader.feed(raw)
            else:
                for begin in range(0, len(raw), chunk):
                    reader.feed(raw[begin:begin + chunk])
            reader.finish()
        # The failure is permanent.
        with self.assertRaises(DecodeError):
            reader.feed(b"")
        with self.assertRaises(DecodeError):
            reader.finish()

    def test_bad_magic(self):
        raw = bytearray(assemble(0, area(rec(0)), 1, 0, 0))
        raw[16] = 3
        self.assert_fails(bytes(raw))

    def test_crc_mismatch(self):
        raw = bytearray(assemble(0, area(rec(0)), 1, 0, 0))
        raw[-1] ^= 0xFF
        self.assert_fails(bytes(raw))

    def test_crc_mismatch_in_filtered_batch_still_fails(self):
        good = assemble(0, area(rec(0)), 1, 0, 0)
        bad = bytearray(assemble(50, area(rec(0)), 1, 0, 0))
        bad[-1] ^= 0xFF
        self.assert_fails(good + bytes(bad), start=0, lso=10)

    def test_crc_mismatch_in_aborted_batch_still_fails(self):
        bad = bytearray(assemble(5, area(rec(0)), 1, 16, 0, producer=7))
        bad[-1] ^= 0xFF
        self.assert_fails(bytes(bad), aborted=[{"producer_id": 7,
                                                "first_offset": 5}])

    def test_batch_length_too_small(self):
        self.assert_fails(struct.pack(">qi", 0, 48))

    def test_negative_last_offset_delta(self):
        self.assert_fails(assemble(0, area(rec(0)), 1, 0, -1))

    def test_negative_records_count(self):
        self.assert_fails(assemble(0, b"", -1, 0, 0))

    def test_negative_base_offset(self):
        self.assert_fails(assemble(-1, area(rec(0)), 1, 0, 0))

    def test_unsupported_compression_codec(self):
        self.assert_fails(assemble(0, b"", 0, 2, 0))

    def test_malformed_gzip(self):
        self.assert_fails(assemble(0, b"notgzip", 0, 1, 0))

    def test_gzip_trailing_garbage(self):
        compressed = gzip.compress(area(rec(0)), mtime=0) + b"\x00"
        self.assert_fails(assemble(0, compressed, 1, 1, 0))

    def test_gzip_incomplete_member(self):
        compressed = gzip.compress(area(rec(0)), mtime=0)
        self.assert_fails(assemble(0, compressed[:-4], 1, 1, 0))

    def test_overlapping_batches(self):
        raw = assemble(0, area(rec(0)), 1, 0, 0) + assemble(0, area(rec(0)), 1,
                                                             0, 0)
        self.assert_fails(raw)

    def test_record_area_trailing_byte(self):
        raw = assemble(0, area(rec(0)) + b"\x00", 1, 0, 0)
        self.assert_fails(raw)

    def test_record_length_too_long(self):
        good = rec(0)
        body = good[1:]
        raw = assemble(0, writer.vint(len(body) + 5) + body, 1, 0, 0)
        self.assert_fails(raw)

    def test_record_length_mismatch(self):
        body = b"\x00\x00\x00\x01\x01\x00"
        raw = assemble(0, writer.vint(len(body) + 1) + body + b"\x00",
                       1, 0, 0)
        self.assert_fails(raw)

    def test_negative_record_length(self):
        self.assert_fails(assemble(0, b"\x01", 1, 0, 0))

    def test_negative_value_length(self):
        body = b"\x00\x00\x00\x01\x03\x00"  # value length zig-zag -2
        raw = assemble(0, writer.vint(len(body)) + body, 1, 0, 0)
        self.assert_fails(raw)

    def test_null_header_key(self):
        body = b"\x00\x00\x00\x01\x01\x01\x01\x00"
        raw = assemble(0, writer.vint(len(body)) + body, 1, 0, 0)
        self.assert_fails(raw)

    def test_negative_header_count(self):
        body = b"\x00\x00\x00\x01\x01\x01"
        raw = assemble(0, writer.vint(len(body)) + body, 1, 0, 0)
        self.assert_fails(raw)

    def test_varint_exceeds_five_octets(self):
        raw = assemble(0, b"\x80\x80\x80\x80\x80\x00", 1, 0, 0)
        self.assert_fails(raw)

    def test_varint_signed32_overflow(self):
        raw = assemble(0, b"\x80\x80\x80\x80\x10", 1, 0, 0)
        self.assert_fails(raw)

    def test_varint_truncated(self):
        self.assert_fails(assemble(0, b"\x80", 1, 0, 0))

    def test_varlong_exceeds_ten_octets(self):
        body = b"\x00" + b"\x80" * 10 + b"\x00" + b"\x00\x01\x01\x00"
        raw = assemble(0, writer.vint(len(body)) + body, 1, 0, 0)
        self.assert_fails(raw)

    def test_varlong_signed64_overflow(self):
        body = b"\x00" + b"\x80" * 9 + b"\x02" + b"\x00\x01\x01\x00"
        raw = assemble(0, writer.vint(len(body)) + body, 1, 0, 0)
        self.assert_fails(raw)

    def test_timestamp_signed64_overflow(self):
        raw = assemble(0, area(rec(0, timestamp=1)), 1, 0, 0,
                       first_ts=INT64_MAX, max_ts=INT64_MAX)
        self.assert_fails(raw)

    def test_offset_delta_exceeds_last(self):
        self.assert_fails(assemble(0, area(rec(3)), 1, 0, 2))

    def test_duplicate_offset_delta(self):
        self.assert_fails(assemble(0, area(rec(0), rec(0)), 2, 0, 0))

    def test_negative_offset_delta(self):
        body = b"\x00\x00\x01\x01\x01\x00"
        raw = assemble(0, writer.vint(len(body)) + body, 1, 0, 0)
        self.assert_fails(raw)

    def test_offset_range_overflow(self):
        self.assert_fails(assemble(INT64_MAX, area(rec(0)), 1, 0, 1))

    def test_truncated_input_at_finish(self):
        raw = assemble(0, area(rec(0)), 1, 0, 0)
        self.assert_fails(raw[:-1])

    def test_truncated_after_complete_batch(self):
        complete = assemble(0, area(rec(0)), 1, 0, 0)
        reader = Reader(0, 100, [])
        self.assertEqual(offsets(reader.feed(complete)), [0])
        self.assertEqual(reader.feed(assemble(1, area(rec(0)), 1, 0, 0)[:12]), [])
        with self.assertRaises(DecodeError):
            reader.finish()

    def test_control_batch_must_be_transactional(self):
        self.assert_fails(assemble(0, area(control_record(0)), 1, 32, 0,
                                   producer=1))

    def test_control_batch_must_be_uncompressed(self):
        compressed = gzip.compress(area(control_record(0)), mtime=0)
        self.assert_fails(assemble(0, compressed, 1, 16 | 32 | 1, 0,
                                   producer=1))

    def test_control_batch_must_have_one_record(self):
        raw = assemble(0, area(control_record(0),
                               rec(1, key=struct.pack(">hh", 0, 0),
                                   value=struct.pack(">hi", 0, 0))),
                       2, 16 | 32, 1, producer=1)
        self.assert_fails(raw)

    def test_control_record_must_be_at_delta_zero(self):
        raw = assemble(0, area(rec(1, key=struct.pack(">hh", 0, 0),
                                   value=struct.pack(">hi", 0, 0))),
                       1, 16 | 32, 1, producer=1)
        self.assert_fails(raw)

    def test_control_bad_key_length(self):
        self.assert_fails(assemble(
            0, area(control_record(key=b"abc")), 1, 16 | 32, 0, producer=1))

    def test_control_bad_key_version(self):
        self.assert_fails(assemble(
            0, area(control_record(key_version=1)), 1, 16 | 32, 0,
            producer=1))

    def test_control_bad_type(self):
        self.assert_fails(assemble(
            0, area(control_record(key=struct.pack(">hh", 0, 2))), 1,
            16 | 32, 0, producer=1))

    def test_control_bad_value(self):
        self.assert_fails(assemble(
            0, area(control_record(value=b"\x00\x00")), 1, 16 | 32, 0,
            producer=1))

    def test_control_bad_value_version(self):
        self.assert_fails(assemble(
            0, area(control_record(value_version=1)), 1, 16 | 32, 0,
            producer=1))

    def test_control_null_value(self):
        raw = assemble(0, area(rec(0, key=struct.pack(">hh", 0, 0),
                                   value=None)), 1, 16 | 32, 0, producer=1)
        self.assert_fails(raw)

    def test_feed_after_finish_fails_permanently(self):
        reader = Reader(0, 100, [])
        reader.feed(assemble(0, area(rec(0)), 1, 0, 0))
        self.assertIsNone(reader.finish())
        self.assertIsNone(reader.finish())  # repeated successful finish
        with self.assertRaises(DecodeError):
            reader.feed(b"")
        with self.assertRaises(DecodeError):
            reader.finish()


class WarehouseValidation(unittest.TestCase):

    def test_build_rows_rejects_non_increasing_offsets(self):
        records = [{"offset": 2, "timestamp": 0, "key": None, "value": None,
                    "headers": []},
                   {"offset": 1, "timestamp": 0, "key": None, "value": None,
                    "headers": []}]
        with self.assertRaises(warehouse.RecordShapeError):
            warehouse.build_rows(records)

    def test_build_rows_rejects_bad_types(self):
        bad = [{"offset": 0, "timestamp": 0, "key": "x", "value": None,
                "headers": []}]
        with self.assertRaises(warehouse.RecordShapeError):
            warehouse.build_rows(bad)
        with self.assertRaises(warehouse.RecordShapeError):
            warehouse.build_rows([{"offset": 0, "timestamp": 0,
                                   "key": None, "value": None,
                                   "headers": [("x", None)]}])
        with self.assertRaises(warehouse.RecordShapeError):
            warehouse.build_rows([{"offset": 0, "timestamp": 0,
                                   "key": None, "value": None,
                                   "headers": [], "extra": 1}])

    def test_build_rows_partition_and_empty(self):
        self.assertEqual(warehouse.build_rows([], partition=3), [])
        record = {"offset": 0, "timestamp": 0, "key": b"", "value": b"",
                  "headers": []}
        self.assertEqual(warehouse.build_rows([record], partition=3),
                         [{"partition": 3, "offset": 0, "timestamp": 0,
                           "key": "", "value": "", "headers": []}])


if __name__ == "__main__":
    unittest.main()
