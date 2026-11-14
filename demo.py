"""End-to-end demo: slice a real writer.py capture and show visible rows."""

import hashlib
import json

import warehouse
import writer
from kafka_capture import Reader

SLICE = 7
START_OFFSET = 40


def extract(raw, slice_size):
    reader = Reader(START_OFFSET, writer.LAST_STABLE, writer.ABORTED)
    records = []
    for begin in range(0, len(raw), slice_size):
        records.extend(reader.feed(raw[begin:begin + slice_size]))
    reader.finish()
    return records


EXPECTED_ROWS = [
    {
        "partition": 0,
        "offset": 41,
        "timestamp": 996,
        "key": None,
        "value": b"keep-B".hex(),
        "headers": [[b"x".hex(), None], [b"x".hex(), b"2".hex()]],
    },
    {
        "partition": 0,
        "offset": 43,
        "timestamp": 1000,
        "key": None,
        "value": None,
        "headers": [],
    },
]


def main():
    raw = writer.capture()
    records = extract(raw, SLICE)
    rows = warehouse.build_rows(records)
    assert rows == EXPECTED_ROWS, (rows, EXPECTED_ROWS)

    # Every split, including single-byte feeds, must agree with the whole feed.
    for slice_size in range(1, len(raw) + 2):
        assert warehouse.build_rows(extract(raw, slice_size)) == rows

    summary = {
        "source": "writer.capture()",
        "bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "feed_slice": SLICE,
        "start_offset": START_OFFSET,
        "last_stable_offset": writer.LAST_STABLE,
        "aborted": writer.ABORTED,
        "visible_offsets": [row["offset"] for row in rows],
        "rows": rows,
        "notes": [
            "offset 40 is hidden: producer 7 aborted interval starts at 40",
            "offset 42 ABORT marker emits nothing and ends the interval",
            "offset 43 is producer 7's later transaction after the ABORT: visible",
            "offset 44 COMMIT marker emits nothing",
            "offset 45 is at the LSO and outside the stable prefix",
        ],
    }
    print("KAFKA_CAPTURE_DEMO_OK")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
