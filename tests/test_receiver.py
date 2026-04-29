import json
import pytest
import requests
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock, Mock, patch

from receiver import parse_bitrate_mbps, parse_datetime_to_unix

NOTIF_ID = "test-notif-001"

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_kafka_bridge():
    mock = MagicMock()
    mock.produce = MagicMock(return_value=True)
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def mock_nf_registry():
    mock = MagicMock()
    mock.add = MagicMock()
    mock.remove = MagicMock(return_value=True)
    mock.all = MagicMock(return_value=[])
    mock.get = MagicMock(return_value=None)
    return mock


@pytest.fixture
def client(mock_kafka_bridge, mock_nf_registry):
    with patch("receiver.PyKafBridge", return_value=mock_kafka_bridge), \
         patch("receiver.nf_registry", mock_nf_registry):
        from receiver import app
        with TestClient(app) as c:
            yield c


@pytest.fixture
def client_with_sub(mock_kafka_bridge, mock_nf_registry):
    """Client where notifId is already registered with snssai+dnn context."""
    mock_nf_registry.get.return_value = {
        "notif_id": NOTIF_ID,
        "snssai": {"sst": 1, "sd": "000001"},
        "dnn": "internet",
        "events": ["PERF_DATA", "UE_MOBILITY"],
        "nef_sub_id": "nef-sub-abc",
        "nef_url": "http://nef:8090/nnef-event-exposure/v1/subscriptions",
        "created_at": 1000000,
    }
    with patch("receiver.PyKafBridge", return_value=mock_kafka_bridge), \
         patch("receiver.nf_registry", mock_nf_registry):
        from receiver import app
        with TestClient(app) as c:
            yield c


# ── Parser unit tests ─────────────────────────────────────────────────────────

class TestParseBitrateMbps:
    def test_mbps(self):
        assert parse_bitrate_mbps("48.57 Mbps") == pytest.approx(48.57)

    def test_gbps(self):
        assert parse_bitrate_mbps("10 Gbps") == pytest.approx(10000.0)

    def test_kbps(self):
        assert parse_bitrate_mbps("500 Kbps") == pytest.approx(0.5)

    def test_bps(self):
        assert parse_bitrate_mbps("1000000 bps") == pytest.approx(1.0)

    def test_tbps(self):
        assert parse_bitrate_mbps("1 Tbps") == pytest.approx(1e6)

    def test_invalid(self):
        assert parse_bitrate_mbps("not a bitrate") is None

    def test_missing_unit(self):
        assert parse_bitrate_mbps("100") is None


class TestParseDatetimeToUnix:
    def test_utc_z(self):
        result = parse_datetime_to_unix("2026-04-20T10:15:00Z")
        assert result == 1776680100

    def test_utc_offset(self):
        result = parse_datetime_to_unix("2026-04-20T10:15:00+00:00")
        assert result == 1776680100

    def test_non_utc_offset(self):
        # +02:00 offset → same instant as 08:15:00Z
        result = parse_datetime_to_unix("2026-04-20T10:15:00+02:00")
        assert result == 1776672900

    def test_invalid(self):
        assert parse_datetime_to_unix("not a date") is None

    def test_empty(self):
        assert parse_datetime_to_unix("") is None


# ── POST /nef/subscriptions ───────────────────────────────────────────────────

class TestCreateNefSubscription:
    @patch("receiver.requests.post")
    def test_success(self, mock_post, client, mock_nf_registry):
        mock_resp = Mock()
        mock_resp.json.return_value = {"subscriptionId": "nef-sub-123"}
        mock_resp.raise_for_status = Mock()
        mock_post.return_value = mock_resp

        response = client.post("/nef/subscriptions", json={
            "notifId": NOTIF_ID,
            "nefUrl": "http://nef:8090/nnef-event-exposure/v1/subscriptions",
            "events": ["PERF_DATA", "UE_MOBILITY"],
            "snssai": {"sst": 1, "sd": "000001"},
            "dnn": "internet",
        })

        assert response.status_code == 201
        data = response.json()
        assert data["notifId"] == NOTIF_ID
        assert data["nefSubscriptionId"] == "nef-sub-123"
        mock_nf_registry.add.assert_called_once()

    @patch("receiver.requests.post")
    def test_nef_timeout(self, mock_post, client, mock_nf_registry):
        mock_post.side_effect = requests.exceptions.Timeout()
        response = client.post("/nef/subscriptions", json={
            "notifId": NOTIF_ID,
            "nefUrl": "http://nef:8090/nnef-event-exposure/v1/subscriptions",
            "events": ["PERF_DATA"],
        })
        assert response.status_code == 504
        mock_nf_registry.add.assert_not_called()

    @patch("receiver.requests.post")
    def test_nef_connection_error(self, mock_post, client, mock_nf_registry):
        mock_post.side_effect = requests.exceptions.ConnectionError()
        response = client.post("/nef/subscriptions", json={
            "notifId": NOTIF_ID,
            "nefUrl": "http://nef:8090/nnef-event-exposure/v1/subscriptions",
            "events": ["PERF_DATA"],
        })
        assert response.status_code == 502
        mock_nf_registry.add.assert_not_called()


# ── GET /nef/subscriptions ────────────────────────────────────────────────────

class TestListNefSubscriptions:
    def test_empty(self, client, mock_nf_registry):
        mock_nf_registry.all.return_value = []
        response = client.get("/nef/subscriptions")
        assert response.status_code == 200
        assert response.json() == {"subscriptions": []}

    def test_with_entries(self, client, mock_nf_registry):
        mock_nf_registry.all.return_value = [
            {"notif_id": "sub-1", "events": ["PERF_DATA"]},
            {"notif_id": "sub-2", "events": ["UE_MOBILITY"]},
        ]
        response = client.get("/nef/subscriptions")
        assert response.status_code == 200
        assert len(response.json()["subscriptions"]) == 2


# ── DELETE /nef/subscriptions/{notif_id} ─────────────────────────────────────

class TestDeleteNefSubscription:
    @patch("receiver.requests.delete")
    def test_success(self, mock_delete, client, mock_nf_registry):
        mock_nf_registry.get.return_value = {
            "nef_sub_id": "nef-sub-abc",
            "nef_url": "http://nef:8090/nnef-event-exposure/v1/subscriptions",
        }
        mock_delete.return_value = Mock(status_code=204)
        response = client.delete(f"/nef/subscriptions/{NOTIF_ID}")
        assert response.status_code == 204
        mock_nf_registry.remove.assert_called_once_with(NOTIF_ID)

    def test_not_found(self, client, mock_nf_registry):
        mock_nf_registry.get.return_value = None
        response = client.delete(f"/nef/subscriptions/unknown-id")
        assert response.status_code == 404


# ── POST /nef/notify ─────────────────────────────────────────────────────────

class TestNefNotify:
    def test_missing_notif_id(self, client):
        response = client.post("/nef/notify", json={"eventNotifs": []})
        assert response.status_code == 400

    def test_unknown_notif_id(self, client, mock_nf_registry):
        mock_nf_registry.get.return_value = None
        response = client.post("/nef/notify", json={"notifId": "unknown", "eventNotifs": []})
        assert response.status_code == 403

    def test_perf_data_event(self, client_with_sub, mock_kafka_bridge):
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "PERF_DATA",
                "timeStamp": "2026-04-20T10:15:00Z",
                "perfDataInfos": [{
                    "ueIpAddr": {"ipv4Addr": "10.0.1.10"},
                    "appId": "app-test",
                    "timeStamp": "2026-04-20T10:15:00Z",
                    "perfData": {
                        "thrputUl": "11.74 Mbps",
                        "thrputDl": "87.57 Mbps",
                        "pdb": 18,
                        "plr": 17,
                    },
                }],
            }],
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_called_once()

        batch = json.loads(mock_kafka_bridge.produce.call_args[0][1])
        assert isinstance(batch, list)
        rec = batch[0]
        assert rec["event"] == "PERF_DATA"
        assert rec["tags"]["ueIpv4Addr"] == "10.0.1.10"
        assert rec["tags"]["snssai_sst"] == 1
        assert rec["tags"]["snssai_sd"] == "000001"
        assert rec["tags"]["dnn"] == "internet"
        assert rec["metrics"]["thrputUl_mbps"] == pytest.approx(11.74)
        assert rec["metrics"]["thrputDl_mbps"] == pytest.approx(87.57)
        assert rec["metrics"]["pdb_ms"] == 18
        assert rec["metrics"]["plr_per_thousand"] == 17

    def test_ue_mobility_event(self, client_with_sub, mock_kafka_bridge):
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "UE_MOBILITY",
                "timeStamp": "2026-04-20T10:15:00Z",
                "ueMobilityInfos": [{
                    "supi": "imsi-001011234567890",
                    "ueTrajs": [
                        {
                            "ts": "2026-04-20T10:14:50Z",
                            "location": {"nrLocation": {
                                "tai": {"plmnId": {"mcc": "001", "mnc": "01"}, "tac": "000001"},
                                "ncgi": {"plmnId": {"mcc": "001", "mnc": "01"}, "nrCellId": "000000001"},
                            }},
                        },
                        {
                            "ts": "2026-04-20T10:15:00Z",
                            "location": {"nrLocation": {
                                "tai": {"plmnId": {"mcc": "001", "mnc": "01"}, "tac": "000002"},
                                "ncgi": {"plmnId": {"mcc": "001", "mnc": "01"}, "nrCellId": "000000002"},
                            }},
                        },
                    ],
                }],
            }],
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_called_once()

        batch = json.loads(mock_kafka_bridge.produce.call_args[0][1])
        rec = batch[0]
        assert rec["event"] == "UE_MOBILITY"
        assert rec["tags"]["supi"] == "imsi-001011234567890"
        assert len(rec["metrics"]["trajectory"]) == 2
        assert rec["metrics"]["trajectory"][0]["tac"] == "000001"
        assert rec["metrics"]["trajectory"][1]["nrCellId"] == "000000002"

    def test_ue_comm_event(self, client_with_sub, mock_kafka_bridge):
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "UE_COMM",
                "timeStamp": "2026-04-20T10:15:00Z",
                "ueCommInfos": [{
                    "supi": "imsi-001011234567890",
                    "comms": [{
                        "startTime": "2026-04-20T10:00:00Z",
                        "endTime": "2026-04-20T10:15:00Z",
                        "ulVol": 1048576,
                        "dlVol": 52428800,
                    }],
                }],
            }],
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_called_once()

        batch = json.loads(mock_kafka_bridge.produce.call_args[0][1])
        rec = batch[0]
        assert rec["event"] == "UE_COMM"
        assert rec["tags"]["supi"] == "imsi-001011234567890"
        assert rec["metrics"]["comms"][0]["ulVol"] == 1048576
        assert rec["metrics"]["comms"][0]["dlVol"] == 52428800

    def test_no_ue_identifier_drops_record(self, client_with_sub, mock_kafka_bridge):
        """PERF_DATA without ueIpAddr and no context tags → record dropped."""
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "PERF_DATA",
                "timeStamp": "2026-04-20T10:15:00Z",
                "perfDataInfos": [{
                    "timeStamp": "2026-04-20T10:15:00Z",
                    "perfData": {"pdb": 10},
                }],
            }],
        }
        # Override context to have no snssai/dnn so there are also no context tags
        from receiver import nf_registry as reg
        reg.get.return_value = {
            "notif_id": NOTIF_ID,
            "snssai": None,
            "dnn": None,
            "events": ["PERF_DATA"],
            "nef_sub_id": None,
            "nef_url": None,
            "created_at": 1000000,
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_not_called()

    def test_unsupported_event_type_skipped(self, client_with_sub, mock_kafka_bridge):
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "DISPERSION",
                "timeStamp": "2026-04-20T10:15:00Z",
                "dispersionInfos": [],
            }],
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_not_called()

    def test_kafka_unavailable(self, client_with_sub, mock_kafka_bridge):
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "PERF_DATA",
                "timeStamp": "2026-04-20T10:15:00Z",
                "perfDataInfos": [{
                    "ueIpAddr": {"ipv4Addr": "10.0.1.10"},
                    "timeStamp": "2026-04-20T10:15:00Z",
                    "perfData": {"pdb": 10},
                }],
            }],
        }
        with patch("receiver.kafka_bridge", None):
            response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_not_called()

    def test_multi_event_batch(self, client_with_sub, mock_kafka_bridge):
        """Both PERF_DATA and UE_MOBILITY in one notification → single Kafka batch."""
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [
                {
                    "event": "PERF_DATA",
                    "timeStamp": "2026-04-20T10:15:00Z",
                    "perfDataInfos": [{
                        "ueIpAddr": {"ipv4Addr": "10.0.1.10"},
                        "timeStamp": "2026-04-20T10:15:00Z",
                        "perfData": {"thrputDl": "50 Mbps"},
                    }],
                },
                {
                    "event": "UE_MOBILITY",
                    "timeStamp": "2026-04-20T10:15:00Z",
                    "ueMobilityInfos": [{
                        "supi": "imsi-001011234567890",
                        "ueTrajs": [{"ts": "2026-04-20T10:15:00Z", "location": {}}],
                    }],
                },
            ],
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_called_once()
        batch = json.loads(mock_kafka_bridge.produce.call_args[0][1])
        assert len(batch) == 2
        events = {r["event"] for r in batch}
        assert events == {"PERF_DATA", "UE_MOBILITY"}

    def test_kafka_produce_returns_false(self, client_with_sub, mock_kafka_bridge):
        """produce() returning False → no exception, 204 still returned."""
        mock_kafka_bridge.produce.return_value = False
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "PERF_DATA",
                "timeStamp": "2026-04-20T10:15:00Z",
                "perfDataInfos": [{
                    "ueIpAddr": {"ipv4Addr": "10.0.1.10"},
                    "timeStamp": "2026-04-20T10:15:00Z",
                    "perfData": {"pdb": 10},
                }],
            }],
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_called_once()

    def test_kafka_produce_raises(self, client_with_sub, mock_kafka_bridge):
        """produce() raising → no exception, 204 still returned."""
        mock_kafka_bridge.produce.side_effect = RuntimeError("broker down")
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "PERF_DATA",
                "timeStamp": "2026-04-20T10:15:00Z",
                "perfDataInfos": [{
                    "ueIpAddr": {"ipv4Addr": "10.0.1.10"},
                    "timeStamp": "2026-04-20T10:15:00Z",
                    "perfData": {"pdb": 10},
                }],
            }],
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204

    def test_ue_mobility_empty_trajs(self, client_with_sub, mock_kafka_bridge):
        """UE_MOBILITY with empty ueTrajs → record still produced with empty trajectory."""
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "UE_MOBILITY",
                "timeStamp": "2026-04-20T10:15:00Z",
                "ueMobilityInfos": [{
                    "supi": "imsi-001011234567890",
                    "ueTrajs": [],
                }],
            }],
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_called_once()
        batch = json.loads(mock_kafka_bridge.produce.call_args[0][1])
        assert batch[0]["metrics"]["trajectory"] == []

    def test_perf_data_context_tags_only(self, client_with_sub, mock_kafka_bridge):
        """PERF_DATA without ueIpAddr but with snssai context → record produced."""
        payload = {
            "notifId": NOTIF_ID,
            "eventNotifs": [{
                "event": "PERF_DATA",
                "timeStamp": "2026-04-20T10:15:00Z",
                "perfDataInfos": [{
                    "timeStamp": "2026-04-20T10:15:00Z",
                    "perfData": {"pdb": 10},
                }],
            }],
        }
        response = client_with_sub.post("/nef/notify", json=payload)
        assert response.status_code == 204
        mock_kafka_bridge.produce.assert_called_once()
        batch = json.loads(mock_kafka_bridge.produce.call_args[0][1])
        rec = batch[0]
        assert rec["tags"]["snssai_sst"] == 1
        assert "ueIpv4Addr" not in rec["tags"]

    @patch("receiver.requests.delete")
    def test_delete_nef_call_fails_still_removes_local(self, mock_delete, client, mock_nf_registry):
        """If the NEF DELETE call fails, the local subscription is still removed."""
        mock_nf_registry.get.return_value = {
            "nef_sub_id": "nef-sub-abc",
            "nef_url": "http://nef:8090/nnef-event-exposure/v1/subscriptions",
        }
        mock_delete.side_effect = Exception("NEF unreachable")
        response = client.delete(f"/nef/subscriptions/{NOTIF_ID}")
        assert response.status_code == 204
        mock_nf_registry.remove.assert_called_once_with(NOTIF_ID)
