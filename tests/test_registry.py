import pytest
from registry import NfRegistry


@pytest.fixture
def reg(tmp_path):
    """Fresh in-memory-like registry backed by a temp file."""
    return NfRegistry(db_path=str(tmp_path / "test.db"))


class TestNfRegistryAdd:
    def test_add_and_get(self, reg):
        reg.add("sub-1", snssai={"sst": 1, "sd": "000001"}, dnn="internet", events=["PERF_DATA"])
        ctx = reg.get("sub-1")
        assert ctx is not None
        assert ctx["notif_id"] == "sub-1"
        assert ctx["snssai"] == {"sst": 1, "sd": "000001"}
        assert ctx["dnn"] == "internet"
        assert ctx["events"] == ["PERF_DATA"]

    def test_add_minimal(self, reg):
        reg.add("sub-min", snssai=None, dnn=None, events=["UE_MOBILITY"])
        ctx = reg.get("sub-min")
        assert ctx["snssai"] is None
        assert ctx["dnn"] is None

    def test_add_with_nef_sub_id(self, reg):
        reg.add("sub-2", snssai=None, dnn=None, events=["UE_COMM"],
                nef_sub_id="nef-abc", nef_url="http://nef/subs")
        ctx = reg.get("sub-2")
        assert ctx["nef_sub_id"] == "nef-abc"
        assert ctx["nef_url"] == "http://nef/subs"

    def test_add_upserts_on_duplicate_notif_id(self, reg):
        reg.add("sub-1", snssai=None, dnn="old", events=["PERF_DATA"])
        reg.add("sub-1", snssai=None, dnn="new", events=["UE_MOBILITY"])
        ctx = reg.get("sub-1")
        assert ctx["dnn"] == "new"
        assert ctx["events"] == ["UE_MOBILITY"]
        assert len(reg.all()) == 1


class TestNfRegistryGet:
    def test_get_missing_returns_none(self, reg):
        assert reg.get("nonexistent") is None

    def test_get_returns_copy(self, reg):
        reg.add("sub-1", snssai={"sst": 1}, dnn="x", events=["PERF_DATA"])
        ctx = reg.get("sub-1")
        ctx["dnn"] = "mutated"
        assert reg.get("sub-1")["dnn"] == "x"


class TestNfRegistryRemove:
    def test_remove_existing(self, reg):
        reg.add("sub-1", snssai=None, dnn=None, events=["PERF_DATA"])
        assert reg.remove("sub-1") is True
        assert reg.get("sub-1") is None

    def test_remove_nonexistent_returns_false(self, reg):
        assert reg.remove("ghost") is False

    def test_remove_deletes_from_db(self, tmp_path):
        db = str(tmp_path / "persist.db")
        r1 = NfRegistry(db_path=db)
        r1.add("sub-1", snssai=None, dnn=None, events=["PERF_DATA"])
        r1.remove("sub-1")

        r2 = NfRegistry(db_path=db)
        assert r2.get("sub-1") is None


class TestNfRegistryAll:
    def test_empty(self, reg):
        assert reg.all() == []

    def test_multiple_entries(self, reg):
        reg.add("s1", snssai=None, dnn=None, events=["PERF_DATA"])
        reg.add("s2", snssai=None, dnn=None, events=["UE_MOBILITY"])
        reg.add("s3", snssai=None, dnn=None, events=["UE_COMM"])
        entries = reg.all()
        assert len(entries) == 3
        ids = {e["notif_id"] for e in entries}
        assert ids == {"s1", "s2", "s3"}


class TestNfRegistryPersistence:
    def test_survives_restart(self, tmp_path):
        db = str(tmp_path / "persist.db")

        r1 = NfRegistry(db_path=db)
        r1.add("sub-1", snssai={"sst": 1, "sd": "abc"}, dnn="internet",
                events=["PERF_DATA", "UE_MOBILITY"], nef_sub_id="nef-xyz",
                nef_url="http://nef/subs")

        r2 = NfRegistry(db_path=db)
        ctx = r2.get("sub-1")
        assert ctx is not None
        assert ctx["snssai"] == {"sst": 1, "sd": "abc"}
        assert ctx["dnn"] == "internet"
        assert ctx["events"] == ["PERF_DATA", "UE_MOBILITY"]
        assert ctx["nef_sub_id"] == "nef-xyz"

    def test_multiple_entries_persist(self, tmp_path):
        db = str(tmp_path / "persist.db")

        r1 = NfRegistry(db_path=db)
        for i in range(5):
            r1.add(f"sub-{i}", snssai=None, dnn=f"dnn-{i}", events=["PERF_DATA"])

        r2 = NfRegistry(db_path=db)
        assert len(r2.all()) == 5
