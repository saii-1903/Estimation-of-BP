# LifeSigns — Kafka & Backend Implementation Guide
**Version:** 1.0  
**Date:** 2026-05-07  
**Scope:** Message broker architecture, topic design, consumer/producer wiring, Docker deployment

---

## 1. Overview

The LifeSigns backend is a Flask HTTP server that receives PPG and vitals data, runs AI inference, and publishes results. Kafka sits between the containerized BLE gateways and this server, and also carries the server's outputs to downstream consumers (dashboards, EHR systems, alert handlers).

**Three Kafka roles in this system:**
1. **Inbound Topic 1** — Raw PPG watch data arriving from BLE gateways
2. **Inbound Topic 2** — Reference BP data (manual cuff readings, dashboard-configured thresholds)
3. **Outbound Topic 3** — Processed results, alerts, and emergency outputs

The Flask server itself is also reachable via direct HTTP POST (for testing or non-Kafka integrations), but production traffic flows through Kafka.

---

## 2. Topic Design

### Topic 1 — `lifesigns.ppg.raw`
**Direction:** BLE Gateway → Flask Server  
**Frequency:** One message every 3 minutes per device (30s of PPG data per packet)  
**Retention:** 1 hour (messages are processed immediately; no replay needed)  
**Partitioning:** Partition by `device_id` — ensures all packets from the same watch arrive in order  

**Message schema:**
```json
{
  "device_id": "WA98AD87",
  "timestamp": 1777961931,
  "payload": { /* full device JSON — same format as HTTP POST body */ }
}
```

The `payload` field contains the raw device JSON exactly as described in `API_Integration_Guide.md`. The outer wrapper (`device_id`, `timestamp`) is added by the BLE gateway before publishing.

---

### Topic 2 — `lifesigns.reference.bp`
**Direction:** Dashboard/BP Device Gateway → Flask Server  
**Frequency:** On-demand — arrives at session start and after each alert  
**Retention:** 24 hours  
**Partitioning:** Partition by `device_id`  

**Message schema:**
```json
{
  "device_id": "WA98AD87",
  "timestamp": 1777961931,
  "ref_sbp": 122,
  "ref_dbp": 78,
  "drift_threshold_sbp": 20,
  "drift_threshold_dbp": 20,
  "source": "NISO204",
  "payload": { /* optional: full device JSON if PPG data accompanies the reference BP */ }
}
```

**Key fields:**
| Field | Required | Description |
|-------|----------|-------------|
| `device_id` | Yes | Identifies which watch session this calibrates |
| `ref_sbp` | Yes | Manually measured systolic BP (mmHg) |
| `ref_dbp` | Yes | Manually measured diastolic BP (mmHg) |
| `drift_threshold_sbp` | No | mmHg deviation that triggers SBP alert (default: 20) |
| `drift_threshold_dbp` | No | mmHg deviation that triggers DBP alert (default: 20) |
| `source` | No | Which BP device took the measurement |
| `payload` | No | If the BP device also has PPG, include it for baseline computation |

---

### Topic 3 — `lifesigns.output`
**Direction:** Flask Server → Downstream Consumers  
**Frequency:** Varies — one message per 15-minute session (normal), immediate on alert  
**Retention:** 7 days  
**Partitioning:** Partition by `device_id`  

**Message types published to Topic 3:**

#### 3a — Normal 15-minute output
```json
{
  "type": "session_result",
  "device_id": "WA98AD87",
  "timestamp": 1777963200,
  "sbp": 118.5,
  "dbp": 74.2,
  "bp": "118.5/74.2",
  "category": "normal",
  "hb": 12.3,
  "glucose": 98.4,
  "session_summary": {
    "total_readings": 5,
    "good_readings": 4,
    "outliers_removed": 1,
    "session_duration_minutes": 15
  }
}
```

#### 3b — Alert (drift detected from calibrated baseline)
```json
{
  "type": "alert",
  "device_id": "WA98AD87",
  "timestamp": 1777963200,
  "reason": "BP drift detected from calibrated baseline",
  "current_sbp": 148.0,
  "baseline_sbp": 122.0,
  "drift_sbp": 26.0,
  "action_required": "Send new reference BP reading for this device"
}
```

#### 3c — Critical alert (still drifting after recalibration)
```json
{
  "type": "critical_alert",
  "device_id": "WA98AD87",
  "timestamp": 1777963200,
  "reason": "BP still drifting after recalibration — entering emergency mode"
}
```

#### 3d — Emergency output (3-reading consensus)
```json
{
  "type": "emergency_output",
  "device_id": "WA98AD87",
  "timestamp": 1777963200,
  "sbp": 152.3,
  "dbp": 96.1,
  "bp": "152.3/96.1",
  "note": "Immediate output — 2+ of 3 readings consistent during alert",
  "readings_used": 3
}
```

#### 3e — Critical alert (emergency readings inconsistent)
```json
{
  "type": "critical_alert",
  "device_id": "WA98AD87",
  "timestamp": 1777963200,
  "reason": "All 3 emergency readings inconsistent — manual check required",
  "readings_sbp": [148.2, 162.5, 131.0]
}
```

#### 3f — Skipped window (motion detected)
```json
{
  "type": "skipped",
  "device_id": "WA98AD87",
  "timestamp": 1777963200,
  "reason": "Motion detected — window discarded"
}
```

---

## 3. Consumer Configuration

### Consumer 1 — PPG Watch Data (`lifesigns.ppg.raw`)

```
Group ID:        lifesigns-ppg-consumer
Auto offset:     earliest (process any missed packets on restart)
Max poll records: 1 (process one packet at a time — inference takes 3–8s)
Session timeout:  30000ms
```

**Processing loop:**
```
Read message from Topic 1
  → Extract device_id and raw payload
  → Call process_vitals(payload)
  → Determine device mode (normal / escalation / emergency)
  → Route result to session aggregator or emergency buffer
  → If session ready: check drift, publish to Topic 3
  → Commit offset
```

**Concurrency:** One consumer thread per device partition. At most N concurrent inferences where N = number of Kafka partitions. Each partition processes one device's stream serially.

---

### Consumer 2 — Reference BP (`lifesigns.reference.bp`)

```
Group ID:        lifesigns-refbp-consumer
Auto offset:     earliest
Max poll records: 1
```

**Processing loop:**
```
Read message from Topic 2
  → Extract device_id, ref_sbp, ref_dbp, thresholds
  → Call handle_reference_bp(device_id, ref_sbp, ref_dbp, payload, thresholds)
  → Update _device_offsets, _device_baselines, _device_drift_thresholds
  → Reset SessionAggregator for this device
  → Set device mode to "normal"
  → Commit offset
```

---

## 4. Producer Configuration

```
Acks:            all  (wait for all replicas to acknowledge)
Retries:         3
Retry backoff:   500ms
Compression:     gzip
```

All outbound messages published to `lifesigns.output`. Key = `device_id` (ensures ordering by device in downstream consumers).

---

## 5. Docker Deployment Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Network                       │
│                                                         │
│  ┌──────────────┐    Topic 1     ┌─────────────────┐   │
│  │ BLE Gateway  │ ─────────────► │                 │   │
│  │ (per device  │                │   Kafka Broker  │   │
│  │  or per ward)│                │                 │   │
│  └──────────────┘                └────────┬────────┘   │
│                                           │             │
│  ┌──────────────┐    Topic 2              │             │
│  │  Dashboard / │ ─────────────►          │             │
│  │  BP Device   │                         │             │
│  │  Gateway     │                         ▼             │
│  └──────────────┘            ┌────────────────────┐    │
│                              │  Flask Vitals Server│    │
│                              │  (this codebase)    │    │
│                              │  berry_app.py       │    │
│                              │  Consumers 1 + 2    │    │
│                              │  Producer → Topic 3 │    │
│                              └────────────────────┘    │
│                                           │             │
│                                  Topic 3  ▼             │
│                         ┌─────────────────────────┐    │
│                         │ Downstream Consumers:    │    │
│                         │ - Dashboard UI           │    │
│                         │ - Alert handler          │    │
│                         │ - EHR integration        │    │
│                         └─────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Services in `docker-compose.yml`

| Service | Image | Role |
|---------|-------|------|
| `zookeeper` | confluentinc/cp-zookeeper | Kafka coordination |
| `kafka` | confluentinc/cp-kafka | Message broker |
| `vitals-server` | lifesigns/vitals:latest | Flask server + consumers |

**Vitals server environment variables:**
```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_PPG_TOPIC=lifesigns.ppg.raw
KAFKA_REFBP_TOPIC=lifesigns.reference.bp
KAFKA_OUTPUT_TOPIC=lifesigns.output
KAFKA_CONSUMER_GROUP_PPG=lifesigns-ppg-consumer
KAFKA_CONSUMER_GROUP_REFBP=lifesigns-refbp-consumer
SERVER_HOST=0.0.0.0
SERVER_PORT=5001
```

**Startup sequence:**
1. Zookeeper starts
2. Kafka starts, waits for Zookeeper
3. Vitals server starts, creates topics if not exist, starts consumers

---

## 6. Topic Creation (at startup)

```python
from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = [
    NewTopic("lifesigns.ppg.raw",       num_partitions=8,  replication_factor=1,
             config={"retention.ms": "3600000"}),          # 1 hour
    NewTopic("lifesigns.reference.bp",   num_partitions=4,  replication_factor=1,
             config={"retention.ms": "86400000"}),         # 24 hours
    NewTopic("lifesigns.output",         num_partitions=8,  replication_factor=1,
             config={"retention.ms": "604800000"}),        # 7 days
]
```

Partition count = 8 supports up to 8 concurrent device streams per consumer group. Increase if ward size requires more simultaneous devices.

---

## 7. Flask Server Thread Model

The Flask server runs three threads simultaneously:

```
Thread 1: Flask HTTP  — handles direct POST requests (testing / non-Kafka path)
Thread 2: Kafka Consumer 1  — PPG watch stream, runs inference loop
Thread 3: Kafka Consumer 2  — Reference BP stream, runs calibration loop
```

All three threads share the in-memory state dicts:
```
_device_offsets          (calibration offsets per device)
_device_baselines        (reference BP baseline per device)
_device_mode             (normal / escalation / emergency per device)
_device_drift_thresholds (alert thresholds per device, from payload)
_emergency_buffer        (last 3 readings in emergency mode per device)
_session_aggregators     (SessionAggregator instance per device)
```

State is in-memory only. On server restart, all per-device state is lost. The next reference BP message from Topic 2 re-establishes calibration. This is acceptable because:
- Sessions are 15 minutes long
- The BLE gateway replays unacknowledged messages on reconnect
- A new reference BP can be sent at any time to re-calibrate

If persistence across restarts is required in a future version, persist `_device_offsets` and `_device_baselines` to Redis or a lightweight SQLite file.

---

## 8. Health Check

`GET /health` returns:

```json
{
  "status": "ok",
  "kafka_connected": true,
  "active_devices": 3,
  "uptime_seconds": 3842
}
```

Used by Docker HEALTHCHECK and load balancer readiness probes.

---

## 9. Error Handling

| Scenario | Action |
|----------|--------|
| Kafka consumer disconnects | confluent-kafka auto-reconnects; messages reprocessed from last committed offset |
| Inference raises exception | Log error, publish `{"type": "error", "device_id": ..., "reason": "..."}` to Topic 3, commit offset (don't retry bad data) |
| Unknown device type in payload | Return HTTP 400 / publish error to Topic 3 |
| Topic 2 arrives before any PPG data exists | Store calibration, apply when next PPG packet arrives |
| Server restart mid-session | Session aggregator resets; next reference BP re-calibrates |

---

## 10. Scaling

**Horizontal scaling** (multiple vitals-server instances):
- Kafka consumer groups handle partition assignment automatically
- Each server instance gets a subset of device partitions
- Shared in-memory state does NOT work across instances — each instance tracks only its assigned devices
- If scaling is needed, move state to Redis (keyed by `device_id`) so any instance can serve any device

**Current design is single-instance.** This is the correct starting point for a ward-scale deployment (up to ~50 simultaneous devices). Scale horizontally only when device count exceeds single-instance capacity.
