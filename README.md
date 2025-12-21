# pei-nwdaf-data-ingestion

> Project for PEI evaluation 25/26

## Overview

FastAPI-based data ingestion service that receives network analytics data from producers and forwards it to Apache Kafka for downstream processing. Acts as the entry point for network telemetry data in the NWDAF pipeline.

## Technologies

- **FastAPI** 0.121.3 - Web framework for HTTP API
- **Apache Kafka** (via confluent-kafka 2.12.2) - Message streaming
- **Uvicorn** 0.34.0 - ASGI server
- **Python** 3.13
- **WebSockets** 14.1 - Real-time bidirectional communication
- **pytest** 8.3.4 - Testing framework

## Key Features

- **HTTP POST endpoint** (`/receive`) for data ingestion
- **Dual format support**: Batch arrays and fallback single packets
- **Field extraction**: 16 required network performance metrics (timestamp, datarate, latency, RSRP, SINR, RSRQ, CQI, location data, etc.)
- **Kafka integration**: Publishes to `network.data.ingested` topic
- **WebSocket support** (`/ws/ingestion`) for real-time broadcasting
- **Error handling**: Graceful Kafka failures with status reporting
- **Comprehensive testing**: 10+ unit tests with mock-based Kafka testing

## Prerequisites

1. Have the Producer and Comms components available locally
2. Update the producer's main.py:
   ```python
   api_url = "http://localhost:8000/receive"  # For testing locally
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Start Kafka

Run Kafka locally using Docker:
```bash
docker run -p 9092:9092 apache/kafka:4.1.1
```

## Start the FastAPI Receiver

```bash
uvicorn receiver:app --reload --host 0.0.0.0 --port 8000
```

## Test the Receiver

Using curl:
```bash
curl -X POST http://localhost:8000/receive \
  -H "Content-Type: application/json" \
  -d '{"data": {"test": 123}}'
```

Or run the producer:
```bash
python3 [producer-folder]/src/main.py
```

## Docker Deployment

```bash
docker-compose up
```

Service runs on port 7000 (Docker) or 8000 (local).

## Architecture

- Processes network data packets with metadata extraction
- Publishes JSON messages to Kafka topic: `network.data.ingested`
- Broadcasts ingested data to WebSocket clients in real-time
- Multi-client connection management with ping/pong keep-alive
