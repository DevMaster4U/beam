import sys
import types

# Stub bittensor so worker.py's module-level import succeeds without the real
# (heavy, network-installing) package.
bt = types.ModuleType("bittensor")


class FakeWallet:
    @staticmethod
    def add_args(parser):
        pass


class FakeSubtensor:
    @staticmethod
    def add_args(parser):
        pass


class FakeConfig(dict):
    def __init__(self, parser, args=None):
        super().__init__()
        self.subtensor = {}

    def get(self, key, default=None):
        return super().get(key, default)


bt.Wallet = FakeWallet
bt.Subtensor = FakeSubtensor
bt.Config = FakeConfig
sys.modules["bittensor"] = bt

sys.argv = ["worker.py"]  # avoid argparse choking on pytest args

import worker  # noqa: E402

print("IMPORT_OK")

# --- Test _plan_subranges ---
abs_start, abs_end = 817889280, 830472191  # from the real log offer
total = abs_end - abs_start + 1
print(f"chunk total bytes: {total}")

for n in (1, 2, 3, 4, 5, 7):
    ranges = worker._plan_subranges(abs_start, abs_end, n)
    sizes = [b - a + 1 for a, b in ranges]
    covered = sum(sizes)
    # Verify contiguity: each range starts exactly where previous ended+1
    contiguous = all(ranges[i][1] + 1 == ranges[i + 1][0] for i in range(len(ranges) - 1))
    starts_end_match = ranges[0][0] == abs_start and ranges[-1][1] == abs_end
    print(
        f"n={n}: streams={len(ranges)} sizes={sizes} covered={covered} "
        f"contiguous={contiguous} bounds_ok={starts_end_match} "
        f"total_match={covered == total}"
    )
    assert covered == total
    assert contiguous
    assert starts_end_match

# Edge case: chunk smaller than requested streams
small_ranges = worker._plan_subranges(0, 99, 8)  # 100 bytes, 8 streams
small_sizes = [b - a + 1 for a, b in small_ranges]
print(f"small chunk (100 bytes, 8 streams requested): sizes={small_sizes} sum={sum(small_sizes)}")
assert sum(small_sizes) == 100

# Edge case: 1 byte
one_byte = worker._plan_subranges(5, 5, 4)
print(f"1-byte range, 4 streams requested: {one_byte}")
assert one_byte == [(5, 5)] or sum(b - a + 1 for a, b in one_byte) == 1

print("ALL_PLAN_SUBRANGES_TESTS_PASSED")
