# BerryMed (NISO101) Testing Guide

Quick start for sending BerryMed PPG data to the local test dashboard.

---

## What is BerryMed?

- Device type: **NISO101** (or BERRYMED)
- Signal: **100 Hz PPG waveform** (resampled internally to 120 Hz)
- No built-in BP cuff — AI estimates BP from PPG only
- No reference BP data

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

## Send BerryMed Data

### Option 1: Use sample file

```bash
python - <<'EOF'
from confluent_kafka import Producer
import json

p = Producer({"bootstrap.servers": "localhost:9092"})
payload = json.load(open("kafka_samples/berrymed_niso101_input.json"))

# Set patient ID to match active patient
payload["admissionId"] = "patient1"

p.produce("ppg-vitals-topic", key=payload["admissionId"].encode(), value=json.dumps(payload).encode())
p.flush()
print("BerryMed packet sent")
EOF
```

### Option 2: Send 3 packets in sequence

```bash
for i in {1..3}; do
  python - <<'EOF'
from confluent_kafka import Producer
import json
import time

p = Producer({"bootstrap.servers": "localhost:9092"})
payload = json.load(open("kafka_samples/berrymed_niso101_input.json"))
payload["admissionId"] = "patient1"
p.produce("ppg-vitals-topic", key=payload["admissionId"].encode(), value=json.dumps(payload).encode())
p.flush()
print(f"Packet {$i} sent")
time.sleep(1)
EOF
done
```

---

## Expected Output

| After packet | Status |
|---|---|
| 1st | SSE: `inference_result`, session progress "1/3" |
| 2nd | SSE: `inference_result`, session progress "2/3" |
| 3rd | Session aggregates, SSE: `session_complete` with final BP/Hb/Glucose |

### Dashboard shows:
- **SBP/DBP** — AI estimated BP
- **Hb** — Hemoglobin (if BP present)
- **Glucose** — Blood glucose (if BP present)
- **Category** — BP category (Normal, Elevated, Stage 1 HTN, etc.)
- **Pleth waveform** — 600-point downsampled signal

### Files saved:
- `results/berrymed_*.json` — raw payload + AI result + session
- `pleth/*.png` — waveform plot

---

## Corrections Applied (BerryMed)

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
    "deviceType": "NISO101",
    "macAddress": "00:11:22:33:44:55",
    "deviceName": "BerryMed-001"
  },
  "admissionId": "patient1",
  "pleth": {
    "plethWave": [100, 102, 105, 103, 101, ...],
    "samplingRate": 100
  }
}
```

---

## Troubleshooting

| Issue | Fix |
|---|---|
| "Rejecting UNKNOWN device" | Check device.deviceType = "NISO101" |
| No pleth data | Ensure `pleth.plethWave` array is present and has data |
| Blank dashboard | Open browser console (F12) to check SSE connection |
| Session never completes | Send 3 packets (each packet = 1 reading) |
| No files saved | Check `results/` and `pleth/` folders exist |

---

## Quick Test Script

Save as `test_berrymed.sh`:

```bash
#!/bin/bash

echo "BerryMed Quick Test"
echo "===================="

# Make sure server is running
python berry_app.py &
sleep 2

echo "Sending 3 BerryMed packets..."
for i in {1..3}; do
  python vitals_test_producer.py kafka_samples --limit 1 --delay 1
  echo "Packet $i sent, waiting..."
  sleep 2
done

echo "Done. Check http://localhost:5000/test"
echo "Files saved in results/ and pleth/"
```

Run:
```bash
chmod +x test_berrymed.sh
./test_berrymed.sh
```

---

## Optional: Manual BP Reference (LS06)

If you want to test calibration:

```bash
python - <<'EOF'
import requests
import json

# Reference cuff reading
payload = {
  "patient_key": "patient1",
  "ref_sbp": 120,
  "ref_dbp": 75
}

resp = requests.post("http://localhost:5000/api/reference_bp", json=payload)
print(resp.json())
EOF
```

Dashboard shows: "Reference BP confirmed" + calibration snapshot

---

**That's it!** BerryMed is ready for testing. 🎉
