import json

import pytest

from polybot.signals import calibration


@pytest.fixture(autouse=True)
def reset_module_caches():
    calibration.reset_cache()
    yield
    calibration.reset_cache()


class TestBucketing:
    def test_delta_buckets(self):
        assert calibration.bucket_delta(50) == "<75"
        assert calibration.bucket_delta(75) == "75-100"
        assert calibration.bucket_delta(99.99) == "75-100"
        assert calibration.bucket_delta(100) == "100-150"
        assert calibration.bucket_delta(199.99) == "150-200"
        assert calibration.bucket_delta(250) == "200-300"
        assert calibration.bucket_delta(500) == "300+"

    def test_entry_buckets(self):
        assert calibration.bucket_entry(0.40) == "<0.50"
        assert calibration.bucket_entry(0.50) == "0.50-0.60"
        assert calibration.bucket_entry(0.78) == "0.70-0.80"
        assert calibration.bucket_entry(0.85) == "0.85-0.90"
        assert calibration.bucket_entry(0.99) == "0.90+"


class TestSmoothedRate:
    def test_zero_trials_is_half(self):
        assert calibration.smoothed_rate(0, 0) == 0.5

    def test_all_wins(self):
        # 5/5 wins → (5+1)/(5+2) = 6/7 ≈ 0.857
        assert calibration.smoothed_rate(5, 5) == pytest.approx(6 / 7)

    def test_all_losses(self):
        assert calibration.smoothed_rate(0, 5) == pytest.approx(1 / 7)

    def test_typical(self):
        # 4 wins of 5 → (4+1)/(5+2) = 5/7 ≈ 0.714
        assert calibration.smoothed_rate(4, 5) == pytest.approx(5 / 7)


def _make_table(
    delta_x_entry_x_hour=None, delta_x_entry=None, delta=None, global_=(34, 45)
):
    return {
        "version": 1,
        "global": {"trials": global_[1], "wins": global_[0]},
        "buckets": {
            "delta_x_entry_x_hour": delta_x_entry_x_hour or {},
            "delta_x_entry": delta_x_entry or {},
            "delta": delta or {},
        },
    }


class TestLookupHierarchy:
    def test_specific_bucket_wins_when_n_sufficient(self):
        table = _make_table(
            delta_x_entry_x_hour={"100-150_0.70-0.80_14": {"trials": 6, "wins": 5}},
            delta_x_entry={"100-150_0.70-0.80": {"trials": 16, "wins": 14}},
            delta={"100-150": {"trials": 16, "wins": 14}},
        )
        rate, source = calibration.lookup_win_rate(
            table, max_abs_delta=120, entry_price=0.78, hour_utc=14, min_n=5
        )
        # smoothed (5+1)/(6+2) = 0.75
        assert rate == pytest.approx(0.75)
        assert source == "delta_x_entry_x_hour"

    def test_fallback_to_delta_x_entry_when_specific_too_sparse(self):
        table = _make_table(
            delta_x_entry_x_hour={"100-150_0.70-0.80_14": {"trials": 2, "wins": 2}},
            delta_x_entry={"100-150_0.70-0.80": {"trials": 16, "wins": 14}},
            delta={"100-150": {"trials": 16, "wins": 14}},
        )
        rate, source = calibration.lookup_win_rate(
            table, max_abs_delta=120, entry_price=0.78, hour_utc=14, min_n=5
        )
        # (14+1)/(16+2) = 15/18
        assert rate == pytest.approx(15 / 18)
        assert source == "delta_x_entry"

    def test_fallback_to_delta_only(self):
        table = _make_table(
            delta_x_entry={"100-150_0.70-0.80": {"trials": 1, "wins": 1}},
            delta={"100-150": {"trials": 16, "wins": 14}},
        )
        rate, source = calibration.lookup_win_rate(
            table, max_abs_delta=120, entry_price=0.78, hour_utc=14, min_n=5
        )
        assert rate == pytest.approx(15 / 18)
        assert source == "delta"

    def test_fallback_to_global(self):
        table = _make_table(global_=(34, 45))
        rate, source = calibration.lookup_win_rate(
            table, max_abs_delta=120, entry_price=0.78, hour_utc=14, min_n=5
        )
        # (34+1)/(45+2) = 35/47
        assert rate == pytest.approx(35 / 47)
        assert source == "global"

    def test_final_fallback_when_table_empty(self):
        table = {"global": {"trials": 0, "wins": 0}, "buckets": {}}
        rate, source = calibration.lookup_win_rate(
            table, max_abs_delta=120, entry_price=0.78, hour_utc=14,
            min_n=5, fallback=0.65,
        )
        assert rate == 0.65
        assert source == "fallback"


class TestLoadTable:
    def test_returns_none_when_missing(self, tmp_path):
        assert calibration.load_table(tmp_path / "nope.json") is None

    def test_returns_parsed_dict(self, tmp_path):
        path = tmp_path / "table.json"
        payload = {"version": 1, "global": {"trials": 1, "wins": 1}, "buckets": {}}
        path.write_text(json.dumps(payload))
        assert calibration.load_table(path) == payload

    def test_caches_table(self, tmp_path):
        path = tmp_path / "table.json"
        path.write_text(json.dumps({"global": {"trials": 1, "wins": 1}, "buckets": {}}))
        first = calibration.load_table(path)
        # Mutate the file — cached version should still come back
        path.write_text(json.dumps({"global": {"trials": 999, "wins": 999}, "buckets": {}}))
        second = calibration.load_table(path)
        assert first is second

    def test_returns_none_on_invalid_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("{not valid json")
        assert calibration.load_table(path) is None
