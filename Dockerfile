FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y \
    git \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt
RUN wget https://github.com/ATNoG/pei-nwdaf-comms/archive/refs/heads/main.zip -O repo.zip && \
    unzip repo.zip "pei-nwdaf-comms-main/kafka/src/*" -d /tmp && \
    mkdir -p /app/utils && \
    mv /tmp/pei-nwdaf-comms-main/kafka/src/* /app/utils/ && \
    rm -rf repo.zip /tmp/pei-nwdaf-comms-main
COPY receiver.py .

CMD ["uvicorn", "receiver:app", "--host", "0.0.0.0", "--port", "7000"]
