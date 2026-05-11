FROM python:3.11-slim

WORKDIR /app

# System deps for confluent-kafka (librdkafka) and scipy/numpy
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libgomp1 && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY . .

# Verify models are present
RUN python -c "\
from pathlib import Path; \
m = Path('modelsss'); \
assert (m / 'classifier.pkl').exists(), 'classifier.pkl missing'; \
assert (m / 'global_feature_scaler.pkl').exists(), 'global_feature_scaler.pkl missing'; \
print('[OK] Models verified')"

CMD ["python", "vitals_kafka_consumer.py"]
