# NISO204 Testing Guide

Quick start for sending NISO204 PPG data to the local test dashboard.

---

## What is NISO204?

- Device type: **NISO204**
- Signal: **120 Hz PPG waveform** (native)
- **Built-in BP cuff**: Provides reference BP in payload (for mismatch checks)
- **Buffering**: 3 packets (30 seconds) combined before processing
- **No calibration offset until confirmed**

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
3. Status bar shows: "Buffering 0/3 packets..."

---

## Send NISO204 Data (3 packets required)

### Send all 3 packets in sequence

```bash
for i in {1..3}; do
  python - <<'EOF'
from confluent_kafka import Producer
import json
import time

p = Producer({"bootstrap.servers": "localhost:9092"})
payload = json.load(open("kafka_samples/niso204_input.json"))
payload["admissionId"] = "patient1"
p.produce("ppg-vitals-topic", key=payload["admissionId"].encode(), value=json.dumps(payload).encode())
p.flush()
print(f"NISO204 Packet {$i}/3 sent")
EOF
  
  if [ $i -lt 3 ]; then
    echo "Waiting for next packet..."
    sleep 1
  fi
done
```

---

## Expected Output

| After packet | Status | Notes |
|---|---|---|
| 1st | SSE: `buffer_status` "1/3 buffered" | No inference yet |
| 2nd | SSE: `buffer_status` "2/3 buffered" | No inference yet |
| 3rd | Packets combined, SSE: `inference_result` | Pleth data merged, inference runs |
| Session ready | SSE: `session_complete` | Final BP/Hb/Glucose with session summary |

### Dashboard shows:
- **SBP/DBP** — AI estimated BP
- **Cuff BP** — Built-in reference from NISO204 (for comparison)
- **Mismatch alert** — if AI and cuff differ > ±10 mmHg
- **Hb** — Hemoglobin (if BP present)
- **Glucose** — Blood glucose (if BP present)
- **Pleth waveform** — Combined 600-point signal

### Files saved:
- `results/niso204_*.json` — combined payload + AI result + session
- `pleth/*.png` — combined waveform plot

---

## Corrections Applied (NISO204)

- **If SBP > 140**: subtract random 15-20
- **If DBP > 100**: subtract random 15-20
- **No correction** for lower values

Example: AI estimates SBP=145, DBP=105
→ Displayed: SBP≈130, DBP≈92 (after random adjustments)

---

## Device Info in JSON

```json
{
  "DeviceName": "NISO204",
  "DeviceMACAddress": "AA:BB:CC:DD:EE:FF",
  "admissionId": "patient1",
  "Pleth": [100, 102, 105, 103, 101, ...],  // 1200 samples (10 sec @ 120 Hz)
  "BPSystolic": 120,      // Cuff reading (optional, for mismatch check)
  "BPDiastolic": 75
}
```

---

## Buffering Behavior

NISO204 **buffers 3 packets** before processing:

```
Packet 1 (10s)  →  buffered  "1/3"
Packet 2 (10s)  →  buffered  "2/3"
Packet 3 (10s)  →  combined & processed  "inference_result"
                →  session complete  "session_complete"
```

The **combined payload** has:
- Merged plethWave (3600 samples = 30 seconds @ 120 Hz)
- Cuff BP from first packet
- Single inference result

---

## Troubleshooting

| Issue | Fix |
|---|---|
| Status stuck at "buffering" | Make sure all 3 packets have `DeviceName: "NISO204"` |
| Response says "buffering" | This is normal — keep sending packets |
| No inference after 3 packets | Check pleth array is not empty |
| Cuff BP mismatch alert | Expected if cuff and AI differ > 10 mmHg. Take another manual reading. |
| Blank dashboard | Open browser console (F12) for SSE errors |

---

## Calibration Test (with LS06 reference)

```bash
# Send NISO204 packets first (3x)
# ... then send LS06 reference reading

python - <<'EOF'
import requests

# Reference cuff reading (e.g., LS06 device)
payload = {
  "patient_key": "patient1",
  "ref_sbp": 120,
  "ref_dbp": 75
}

resp = requests.post("http://localhost:5000/api/reference_bp", json=payload)
print(resp.json())
EOF

# Now send more NISO204 packets — they'll be calibrated with the offset
```

---

## Notes

- **Buffering is per-patient**: Different patients' packets don't interfere
- **Buffer resets** after 3 packets are combined
- **Pleth waveforms** from all 3 packets are concatenated (no gaps)
- **Cuff BP** is taken from the first packet in the buffer

---

**Ready to test NISO204!** 🎉
