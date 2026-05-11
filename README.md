# PPG Vitals Estimation Pipeline — Kafka Consumer

Real-time cuffless vitals estimation for continuous patient monitoring.
Consumes PPG data from Kafka, runs AI inference, writes results to MongoDB.

```
PPG Device → Kafka Topic → [This Container] → MongoDB
```

---

## What It Does

For every 30-second PPG recording received on Kafka:

1. Extracts PPG signal from the message (`PlethWave[32:]` for NISO204, or `Pleth` for Berry watch)
2. Validates signal — minimum 25 seconds required
3. Runs BP classifier → assigns group (Hypo / Normal / Hyper)
4. Runs per-group XGBoost regressors → SBP and DBP (mmHg)
5. Runs Hb regressor → Hemoglobin (g/dL)
6. Runs Glucose regressor → Blood Glucose (mg/dL)
7. Applies personal calibration offsets if provided
8. Writes structured result document to MongoDB with `processingStatus: null`

---

## Setup

### Step 1 — Get the files

Unzip `LifeSigns_Vitals_Docker.zip`. You will see:

```
LifeSigns_Vitals_Docker/
├── Dockerfile
├── docker-compose.yml
├── .env.template
├── requirements.txt
├── README.md
├── kafka_config.py
├── vitals_kafka_consumer.py
├── vitals_processor.py
├── vitals_mongo_writer.py
├── vitals_standalone.py
├── inference_engine.py
├── config.py
└── modelsss/               ← trained AI models (do not modify)
```

### Step 2 — Configure environment

Copy `.env.template` to `.env` and fill in your values:

```bash
cp .env.template .env
```

Edit `.env`:

```env
KAFKA_BOOTSTRAP_SERVERS=your-kafka-broker:9092
KAFKA_TOPIC=ppg-vitals-topic
KAFKA_GROUP_ID=ppg-vitals-consumer-group
KAFKA_WORKERS=3

MONGO_URI=mongodb://user:password@your-mongo-host:27017
MONGO_DB=vitals_db
MONGO_COLLECTION=ppg_vitals_results

FACILITY_ID=CF0000000000
LOG_LEVEL=INFO
```

### Step 3 — Build the Docker image

```bash
docker build -t lifesigns-vitals:latest .
```

First build takes 3–5 minutes (downloads Python base image, installs packages).

### Step 4 — Run

```bash
docker-compose up -d
```

### Step 5 — Verify it is running

```bash
# Check logs
docker logs -f lifesigns-vitals-vitals-consumer-1
```

You should see:
```
PPG Vitals Consumer started | topic=ppg-vitals-topic group=ppg-vitals-consumer-group workers=3
```

### Stop

```bash
docker-compose down
```

---

## Kafka Message Format (Input)

Publish a JSON message to the configured Kafka topic:

```json
{
  "admissionId":   "ADM819104078",
  "patientId":     "MRN54642783626",
  "facilityId":    "CF1315821527",
  "deviceId":      "BM-001",
  "timestamp":     1770912322923,
  "Age":           45,
  "Gender":        "Male",
  "BMI":           25.9,
  "PlethWave":     [... 3632 values ...],
  "PRAllData":     [72, 73, 71, 74, 75, 76, 76, 76, 76, 76],
  "Reference_SBP": null,
  "Reference_DBP": null,
  "offsets":       {}
}
```

### Field Details

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `admissionId` | string | Yes | Unique admission identifier |
| `patientId` | string | Yes | Patient MRN or ID |
| `facilityId` | string | No | Facility code — falls back to `FACILITY_ID` env var |
| `deviceId` | string | No | Device identifier |
| `timestamp` | number | Yes | Unix timestamp in milliseconds |
| `Age` | number | Yes | Patient age in years |
| `Gender` | string | Yes | `"Male"` or `"Female"` |
| `BMI` | number | No | BMI — or provide `Height` (cm) + `Weight` (kg) |
| `PlethWave` | array | Yes* | NISO204 format — first 32 bytes are header, signal starts at index 32, always 120 Hz |
| `Pleth` | array | Yes* | Berry watch format — raw PPG bytes at 100 Hz |
| `PRAllData` | array | No | Heart rate values at 1 Hz (one per second) |
| `Reference_SBP` | number | No | Cuff SBP reading for calibration (first message only) |
| `Reference_DBP` | number | No | Cuff DBP reading for calibration (first message only) |
| `offsets` | object | No | Carry-forward calibration offsets from previous reading |

*One of `PlethWave` or `Pleth` is required. Minimum signal length: 25 seconds (3000 samples @ 120Hz or 2500 @ 100Hz).

**Signal priority:** `PlethWave[32:]` is used if `len(PlethWave) > 32`, otherwise `Pleth` is used.

---

## MongoDB Document Format (Output)

```json
{
  "uuid":        "b3f1c2d4-a1b2-...",
  "admissionId": "ADM819104078",
  "patientId":   "MRN54642783626",
  "facilityId":  "CF1315821527",
  "deviceId":    "BM-001",
  "timestamp":   1770912322923,

  "input": {
    "age":              45,
    "gender":           "Male",
    "bmi":              25.9,
    "source_hz":        120,
    "input_samples":    3568,
    "signal_source":    "PlethWave[32:]",
    "calibration_used": false
  },

  "vitals": {
    "sbp":         128.4,
    "dbp":         79.1,
    "bp_category": "normal",
    "hb":          13.2,
    "glucose":     98.5
  },

  "offsets":          {},
  "processingStatus": null,
  "processedAt":      null,
  "processedBy":      null,
  "_processing_time_s": 1.8,
  "_processed_utc":     "2025-04-27T10:32:00+00:00"
}
```

### BP Category Values

| Value | Meaning |
|-------|---------|
| `"hypo"` | Hypotension — SBP < 90 or DBP < 60 |
| `"normal"` | Normal — SBP 90–129 and DBP 60–79 |
| `"hyper"` | Hypertension — SBP ≥ 130 or DBP ≥ 80 |

### processingStatus

Written as `null` by this pipeline. Your UI / clinical review system should update it:
- `"processed"` — reviewed by clinician
- `"rejected"` — flagged as invalid

---

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `ppg-vitals-topic` | Topic to consume from |
| `KAFKA_GROUP_ID` | `ppg-vitals-consumer-group` | Consumer group ID |
| `KAFKA_WORKERS` | `3` | Parallel processing threads |
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGO_DB` | `vitals_db` | Database name |
| `MONGO_COLLECTION` | `ppg_vitals_results` | Collection name |
| `FACILITY_ID` | `CF0000000000` | Fallback facility ID if not in message |
| `LOG_LEVEL` | `INFO` | Logging level — `DEBUG`, `INFO`, `WARNING` |

---

## Error Handling

If a message fails (bad signal, inference error), an error document is written to MongoDB:

```json
{
  "uuid":             "...",
  "admissionId":      "ADM819104078",
  "vitals":           null,
  "processingStatus": "error",
  "processingError":  "Insufficient signal: 1200 samples (need 3000)",
  "_processed_utc":   "2025-04-27T10:32:00+00:00"
}
```

The Kafka offset is always committed — a bad message never blocks the queue.

---

## Requirements

- Docker installed and running
- Kafka broker accessible from the container
- MongoDB instance accessible from the container
- No Python installation needed on the host machine — everything runs inside Docker
