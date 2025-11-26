# pei-nwdaf-data-ingestion

## Prerequisites

1. Have the Producer and Comms components available locally.

2. Update the producer’s main.py:
    >api_url = "http://localhost:8000/receive"  # For testing locally


3. Install Python dependencies:

    >pip install -r requirements.txt

## Start Kafka

Run Kafka locally using Docker:

>docker run -p 9092:9092 apache/kafka:4.1.1

## Start the FastAPI Receiver

Run the receiver module:

>uvicorn receiver:app --reload --host 0.0.0.0 --port 8000

## Test the Receiver

You can verify that messages are being received either by:

1. Using curl:

    >curl -X POST http://localhost:8000/receive \
     -H "Content-Type: application/json" \
     -d '{"data": {"test": 123}}'


2. Running the producer script to send data (be sure to have the datasets where needed):
    >python3 [producer-folder]/src/main.py

## TODO

- Provide a solid test set.
- Implement additional filters for incoming data if required.


