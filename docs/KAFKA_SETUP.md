# LifeSigns — Kafka Setup Guide
**Audience:** Engineering and deployment  
**Scope:** What Kafka does in this system, how to configure it, what to send, what comes out

---

## What Kafka Does Here

The BLE gateway (running at the bedside) receives PPG waveform data from the patient's watch over Bluetooth. It parses the raw BLE packets into JSON and publishes one message to Kafka every 3 minutes per patient.

The LifeSigns server runs `vitals_kafka_consumer.py`, which subscribes to that topic, processes each message, and writes results to MongoDB.

```
Watch → BLE Gateway → Kafka topic → vitals_kafka_consumer.py → MongoDB
```

All device types (NISO204, CHECKME/NISO103, BerryMed/NISO101, LS06 reference cuff) publish to **one topic**. The consumer detects the device type from the payload and routes accordingly.

---

## Topic

| Setting | Value |
|---------|-------|
| Topic name | `lifesigns.ppg.raw` (configurable via `KAFKA_TOPIC`) |
| Message key | `admissionId` of the patient |
| Message value | UTF-8 encoded JSON (see sample files) |
| Partitions | ≥ 1 (recommended: number of concurrent consumers) |
| Retention | ≥ 24 hours |

**Why key by `admissionId`?**  
Kafka guarantees that all messages with the same key go to the same partition and are consumed in order. This ensures that packets for the same patient are processed sequentially — required for the session aggregation and state machine logic.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|----------|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | Kafka broker address, e.g. `broker.example.com:9092` |
| `KAFKA_TOPIC` | Yes | Topic name. Default: `lifesigns.ppg.raw` |
| `KAFKA_GROUP_ID` | Yes | Consumer group ID. Default: `lifesigns-vitals-consumer` |
| `MONGO_URI` | Yes | MongoDB connection string |
| `MONGO_DB` | Yes | Database name. Default: `vitals_db` |
| `MONGO_COLLECTION` | Yes | Collection name. Default: `ppg_vitals_results` |
| `FACILITY_ID` | Yes | Facility identifier written to every output document |
| `LOG_LEVEL` | No | `DEBUG`, `INFO`, `WARNING`. Default: `INFO` |
| `BP_MISMATCH_THRESHOLD_SBP` | No | NISO204 per-packet alert threshold (mmHg). Default: `10` |
| `BP_MISMATCH_THRESHOLD_DBP` | No | NISO204 per-packet alert threshold (mmHg). Default: `10` |
| `BP_TREND_THRESHOLD_SBP` | No | Session-to-session trend alert (mmHg). Default: `15` |
| `BP_TREND_THRESHOLD_DBP` | No | Session-to-session trend alert (mmHg). Default: `15` |
| `LS06_CONFIRM_TOLERANCE` | No | LS06 second-reading confirm tolerance (mmHg). Default: `5` |

---

## Running the Consumer

```bash
# Install dependencies
pip install confluent-kafka pymongo python-dotenv

# Set environment (or use .env file)
export KAFKA_BOOTSTRAP_SERVERS=broker.example.com:9092
export KAFKA_TOPIC=lifesigns.ppg.raw
export KAFKA_GROUP_ID=lifesigns-vitals-consumer
export MONGO_URI=mongodb://mongo.example.com:27017

# Start the consumer
python vitals_kafka_consumer.py
```

To run multiple instances for higher throughput, give each a unique `KAFKA_GROUP_ID` or use partitions with the same group ID.

---

## What to Send (Input Message Format)

The BLE gateway publishes JSON to the Kafka topic. See `kafka_samples/` for complete examples.

### Required fields (all device types)

| Field | Type | Description |
|-------|------|-------------|
| `admissionId` | string | **Patient identifier** — used as the Kafka message key and for all per-patient state |
| `patientId` | string | MRN or patient record number |
| `facilityId` | string | Facility code |
| `timestamp` | integer | Unix epoch milliseconds |

### NISO204

Additional fields:

| Field | Type | Description |
|-------|------|-------------|
| `DeviceName` | string | Must be `"NISO204"` for device detection |
| `deviceID` | string | Device MAC address |
| `Pleth` | array | ≥ 3000 float/int samples at **120 Hz** (= 25 seconds minimum) |
| `BPSystolic` | integer | Device cuff reading. Set `404` if not measured this cycle |
| `BPDiastolic` | integer | Device cuff reading. Set `200` if not measured this cycle |

### CHECKME O2 / NISO103

| Field | Type | Description |
|-------|------|-------------|
| `device.deviceType` | string | `"NISO103"` |
| `device.macAddress` | string | Device MAC address |
| `pleth.plethWave` | array | ≥ 3000 samples at **120 Hz** |
| `bp.bpSystolic`, `bp.bpDiastolic` | integer | Present in payload but **ignored** |

### BerryMed Watch / NISO101

| Field | Type | Description |
|-------|------|-------------|
| `device.deviceType` | string | `"NISO101"` |
| `device.macAddress` | string | Device MAC address |
| `pleth.plethWave` | array | ≥ 2500 samples at **100 Hz** (resampled to 120 Hz internally) |
| `acc` | object | Accelerometer data (optional). Used for motion gating if present |
| `bp.bpSystolic`, `bp.bpDiastolic` | integer | Present in payload but **ignored** |

### LS06 Reference Cuff

| Field | Type | Description |
|-------|------|-------------|
| `device.deviceType` | string | `"LS06"` |
| `device.macAddress` | string | Device MAC address |
| `bp.bpSystolic` | integer | **Actual cuff reading** — used by state machine |
| `bp.bpDiastolic` | integer | **Actual cuff reading** — used by state machine |
| `bp.bpError` | integer | `0` = valid. `1` = measurement error (written as error doc, state machine not advanced) |
| `pleth.plethWave` | array | Placeholder — always ignored |

---

## What Comes Out (Output Document Format)

One document per event is written to MongoDB (`vitals_db.ppg_vitals_results`). See `kafka_samples/` for full examples.

| `processingStatus` | When written | Contains |
|--------------------|-------------|---------|
| `session_complete` | Every 15 minutes (5 readings averaged) | `vitals` (sbp, dbp, bp_category, hb, glucose), `offsets`, `bp_alert`, `bp_trend_alert`, `session_summary` |
| `calibration_applied` | Immediately when first non-zero offset is confirmed | Calibrated BP based on latest AI + offset; written so the dashboard doesn't wait 15 min |
| `reference_bp` | When LS06 message arrives | `reference_bp` (sbp/dbp), `bp_alert` if mismatch with AI |
| `error` | Signal too short, noisy, or inference failure | `processingError` reason |
| `skipped` | All 6 motion segments bad | Informational only |

**Hb and Glucose** are included in `session_complete` documents whenever BP is present. They are `null` only if the AI model fails to produce a BP output (same failure condition).

---

## Testing with the Test Producer

To send sample JSON files to Kafka for testing:

```bash
# Send all files in a folder (one message per file, 0.5s delay)
python vitals_test_producer.py data/samples/

# Send specific folder with slower pace
python vitals_test_producer.py data/samples/ --delay 3

# Send only first 5 files
python vitals_test_producer.py data/samples/ --limit 5
```

The test producer adds `admissionId`, `patientId`, `facilityId`, and `deviceId` defaults if they are missing from the JSON file.

---

## How the Consumer Processes a Message

```
Message arrives on topic
    │
    ▼
Detect device type (NISO204 / NISO103 / NISO101 / LS06)
    │
    ├─── LS06? → Extract bp.bpSystolic/bpDiastolic
    │            → Advance reference validation state machine
    │            → Write reference_bp doc to MongoDB
    │            → If newly confirmed with non-zero offset → write calibration_applied doc
    │            └─── stop
    │
    ├─── Error / Skipped? → Write error/skipped doc → stop
    │
    └─── AI inference path (NISO204 / NISO103 / NISO101):
             │
             ├─ Signal validation (≥ 3000 samples)
             ├─ Signal cleaning (sentinel removal, spike removal, resampling)
             ├─ Motion gate (per 5-second segment)
             ├─ AI model → raw SBP, DBP, Hb, Glucose
             ├─ NISO204: per-packet mismatch check vs device cuff reading
             ├─ Apply calibration offset (if state is normal/breach_pending/case2_pending)
             ├─ If breach_pending: compare reading vs pending_ref (post-breach resolution)
             ├─ Add to session buffer (3-min deduplication)
             │
             └─ When 5 readings collected (session complete):
                  ├─ Outlier removal (median ± 10 mmHg)
                  ├─ Session-level drift check (vs baseline)
                  ├─ Trend check (vs previous session)
                  └─ Write session_complete doc to MongoDB
```

---

## Alert Types

| Alert | Field | Trigger |
|-------|-------|---------|
| `reference_mismatch` | `bp_alert` on `reference_bp` doc | First LS06 disagrees with AI by > 10 mmHg |
| `bp_mismatch` | `bp_alert` on session doc | NISO204 cuff vs AI raw > 10 mmHg (per packet) |
| `bp_drift` | `bp_alert` on session doc | Session avg drifts > 10 mmHg from calibrated baseline |
| `bp_drift_escalation` | `bp_alert` on session doc | < 3 of 5 post-breach readings match pending reference |
| `breach_resolved` | `bp_alert` on session doc | Breach recovery confirmed — baseline updated |
| `bp_trend` | `bp_alert` or `bp_trend_alert` | Session-to-session change > 15 mmHg SBP or DBP |

If `bp_drift` and `bp_trend` both fire in the same session: `bp_alert` holds the drift alert and `bp_trend_alert` holds the trend.
