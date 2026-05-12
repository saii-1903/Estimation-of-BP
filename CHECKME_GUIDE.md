# CHECKME (NISO103) Testing Guide

Quick start for sending CHECKME PPG data to the local test dashboard.

---

## What is CHECKME?

- Device type: **NISO103** (or CHECKME)
- Signal: **120 Hz PPG waveform**
- **No built-in BP**: AI estimates BP from PPG only
- **No buffering**: Processes immediately (unlike NISO204)
- **SpO2 data**: Included but not used (AI ignores it)

---

## Start the Test Server

```bash
python berry_app.py
```

Open browser: http://localhost:5000/test

---

## Set Active Patient

1. **Enter patient name** in "Start Receiving" button
2. Click **"Start Receiving"**
3. Status bar shows: "Waiting for 3 readings..."

---

## Send CHECKME Data

### Send single packet (processes immediately)

```bash
python - <<'EOF'
from confluent_kafka import Producer
import json

p = Producer({"bootstrap.servers": "localhost:9092"})
payload = json.load(open("kafka_samples/checkme_niso103_input.json"))
payload["admissionId"] = "patient1"
p.produce("ppg-vitals-topic", key=payload["admissionId"].encode(), value=json.dumps(payload).encode())
p.flush()
print("CHECKME packet sent")
EOF
```

### Send 3 packets in sequence (for session complete)

```bash
for i in {1..3}; do
  python - <<'EOF'
from confluent_kafka import Producer
import json
import time

p = Producer({"bootstrap.servers": "localhost:9092"})
payload = json.load(open("kafka_samples/checkme_niso103_input.json"))
payload["admissionId"] = "patient1"
p.produce("ppg-vitals-topic", key=payload["admissionId"].encode(), value=json.dumps(payload).encode())
p.flush()
print(f"CHECKME Packet {$i}/3 sent")
EOF
  
  if [ $i -lt 3 ]; then
    echo "Waiting for next packet (session requires 3)..."
    sleep 1
  fi
done
```

---

## Expected Output

| After packet | Status | Notes |
|---|---|---|
| 1st | SSE: `inference_result` | Individual reading, no session yet |
| 2nd | SSE: `inference_result` | Individual reading, session progress "2/3" |
| 3rd | Session complete, SSE: `session_complete` | Final BP/Hb/Glucose aggregated |

### Dashboard shows:
- **SBP/DBP** — AI estimated BP
- **Hb** — Hemoglobin (if BP present)
- **Glucose** — Blood glucose (if BP present)
- **Category** — BP category
- **Pleth waveform** — 600-point downsampled signal
- **Signal quality** — PPG signal confidence

### Files saved:
- `results/checkme_*.json` — raw payload + AI result + session
- `pleth/*.png` — waveform plot

---

## Corrections Applied (CHECKME)

- **If SBP < 100**: add random 15-20
- **If DBP < 60**: add random 10-15
- **No modification** for 60 ≤ DBP < 100

Example: AI estimates SBP=95, DBP=55
→ Displayed: SBP≈110, DBP≈68 (after random adjustments)

---

## Device Info in JSON

```json
{
  "device": {
    "deviceType": "NISO103",
    "macAddress": "11:22:33:44:55:66",
    "deviceName": "CHECKME-001"
  },
  "admissionId": "patient1",
  "pleth": {
    "plethWave": [100, 102, 105, 103, 101, ...],  // 1200 samples (10 sec @ 120 Hz)
    "samplingRate": 120
  },
  "spo2": {
    "spo2": 98,
    "pulse": 72
  }
}
```

---

## Key Differences from Other Devices

| Feature | NISO204 | CHECKME | BERRYMED |
|---|---|---|---|
| Buffering | 3 packets | None (immediate) | None (immediate) |
| Built-in BP | Yes (cuff) | No | No |
| Correction (low BP) | N/A | +15-20 if SBP < 100 | +15-20 if SBP < 100 |
| Correction (high BP) | -15-20 if SBP > 140 | N/A | N/A |
| Signal rate | 120 Hz native | 120 Hz native | 100 Hz (resampled) |

---

## Troubleshooting

| Issue | Fix |
|---|---|
| "Rejecting UNKNOWN device" | Check `device.deviceType = "NISO103"` |
| No pleth data | Ensure `pleth.plethWave` array exists and has data |
| Session never completes | Send 3 packets (CHECKME processes each immediately) |
| Blank dashboard | Open browser console (F12) for SSE connection errors |
| Files not saved | Check `results/` and `pleth/` folders |

---

## Calibration Test (with LS06 reference)

```bash
# Send CHECKME packet first
# ... then send LS06 reference reading

python - <<'EOF'
import requests

# Reference cuff reading
payload = {
  "patient_key": "patient1",
  "ref_sbp": 120,
  "ref_dbp": 75
}

resp = requests.post("http://localhost:5000/api/reference_bp", json=payload)
print(resp.json())
EOF

# Send more CHECKME packets — they'll be calibrated
```

---

## Quick Test

```bash
# 1. Start server
python berry_app.py

# 2. In another terminal, send 3 CHECKME packets
python vitals_test_producer.py kafka_samples --limit 3 --delay 1

# 3. Open browser
# http://localhost:5000/test
```

---

**CHECKME is ready to test!** 🎉
