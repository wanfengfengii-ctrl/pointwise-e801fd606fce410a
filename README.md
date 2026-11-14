# Kafka v2 batch fixtures and read_committed extractor

`writer.py` emits real physical magic=2 batches only. `kafka_capture.Reader`
consumes the captured stream and yields visible user records; `warehouse.py`
projects them to warehouse rows.

- `python3 verify.py` — original writer byte checks plus the full capture
  contract suite (`tests_capture.py`, 69 tests).
- `python3 demo.py` — feeds `writer.capture()` in 7-byte slices and prints the
  visible warehouse rows with null/empty bytes kept distinct.

Expected demo result: offsets 41 and 43 are visible; offset 40 is hidden by the
producer-7 aborted interval, markers 42/44 emit nothing (the ABORT at 42 ends
that interval so 43 survives), and offset 45 sits at the LSO.

Implementation notes:

- `kafka_capture.py` — CRC32C, gzip multi-member decoding, exact record
  boundaries, zig-zag varint/varlong bounds, timestamp and offset reconstruction,
  compaction gaps, and producer-scoped aborted-interval visibility.
- `warehouse.py` — output-shape validation; bytes are hex-encoded, `None` stays
  `null` so it never collapses into `""`.
- `writer.py` and `checks.py` are unmodified.
- Standard library only; no Kafka service or client is used.
