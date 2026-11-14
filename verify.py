"""Verification entry point: original writer checks plus capture contract tests."""

import runpy
import sys
import unittest

# checks.py keeps its own assertions and prints KAFKA_BATCH_WRITER_OK.
runpy.run_path("checks.py", run_name="__main__")

import tests_capture  # noqa: E402  (imported after the original byte checks)

suite = unittest.defaultTestLoader.loadTestsFromModule(tests_capture)
result = unittest.TextTestRunner(verbosity=2).run(suite)
if not result.wasSuccessful():
    sys.exit(1)
print("KAFKA_CAPTURE_CONTRACT_OK")
