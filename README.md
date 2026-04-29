# pei-nwdaf-data-ingestion

Receives NEF event notifications (TS 29.591) and publishes normalized records to Kafka.

## How it works

1. Client calls `POST /nef/subscriptions` with a NEF URL, SNSSAI, DNN, and event types
2. Service subscribes to the NEF and stores the context locally
3. NEF pushes callbacks to `POST /nef/notify`; service normalizes `PERF_DATA`, `UE_MOBILITY`, `UE_COMM`
4. Policy transformations applied (field filtering, hashing, redaction) if enabled
5. Records published to `network.data.ingested`

## Running

```bash
cp .env.example .env
docker compose up -d
```

Port: `7000`

## API

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/nef/subscriptions` | Subscribe to a NEF |
| `GET` | `/nef/subscriptions` | List active subscriptions |
| `DELETE` | `/nef/subscriptions/{notif_id}` | Unsubscribe |
| `POST` | `/nef/notify` | NEF callback receiver |
| `WS` | `/ws/ingestion/{notif_id}` | Real-time stream of ingested events |

## Subscribe example

```bash
curl -X POST http://localhost:7000/nef/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "notifId": "sub-001",
    "nefUrl": "http://172.21.0.1:8090/nnef-event-exposure/v1/subscriptions",
    "snssai": {"sst": 1, "sd": "000001"},
    "dnn": "internet",
    "events": ["PERF_DATA", "UE_MOBILITY"]
  }'
```

## Environment

| Variable | Default | Description |
|---|---|---|
| `KAFKA_HOST` | `kafka` | Kafka broker host |
| `KAFKA_PORT` | `9092` | Kafka broker port |
| `POLICY_SERVICE_URL` | `http://policy-service:8788` | Policy service URL |
| `POLICY_ENABLED` | `false` | Enable policy enforcement |
| `HOST` | `data-ingestion` | Hostname used in NEF callback URL |
| `PORT` | `7000` | Service port |
