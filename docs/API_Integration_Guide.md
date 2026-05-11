# LifeSigns API — Hardware Integration Guide
**Version:** 1.0  
**Date:** 2026-05-05  
**Contact:** saisheashan@lifesigns.in  



## 1. Overview

The LifeSigns API vitals server exposes a single HTTP endpoint that accepts PPG (photoplethysmography) and vitals data from medical devices. Upon receiving data, the server:

1. Stores the raw record
2. Runs AI inference on the PPG signal to estimate Blood Pressure, Hemoglobin, and Glucose
3. Returns the AI-estimated results immediately in the HTTP response

---

## 2. Endpoint Details

| Property | Value |
|----------|-------|
| **URL** | `http://172.16.22.247:5001/api/receive_data` |
| **Method** | `POST` |
| **Content-Type** | `application/json` |
| **Authentication** | None |
| **Response Format** | JSON |
| **Timeout** | **Set client timeout to minimum 30 seconds.** AI inference takes 3–8 seconds per request. |



---

## 3. Request Body — Three Supported Device Formats

The endpoint auto-detects your device type from the JSON structure. Three formats are supported:

---

### Device 1 — NISO204

The NISO204 device outputs a flat JSON structure with a `DeviceName` field.

- **PPG field:** `Pleth` (large ADC float values, e.g. 647764.0)
- **Sampling rate:** 200 Hz (specified in `FS` field)
- **Detection:** `DeviceName == "NISO204"`

```json
{
    "DeviceName":  "NISO204",
    "BPSystolic":  122,
    "BPDiastolic": 78,
    "Pleth":       [647764.0, 648201.5, 649033.2, 648750.1, ...],

    "Name":   "John Doe",
    "Age":    35,
    "Gender": "Male",
    "Date":   "2026-05-05",

    "BMI":    24.5,
    "Height": 170,
    "Weight": 70,

    "FS":        200,
    "PRAllData": [72, 74, 73, 75, 71, 72, 74, 73, ...]
}
```

> **Send:** minimum 5000 samples, recommended **6000 samples** (30s × 200 Hz → resampled to 3600 @ 120 Hz)

---

### Device 2 — NISO103

The CHECKME device outputs a nested JSON structure with `device.deviceType` set to `"NISO103"`.

- **PPG field:** `pleth.plethWave` (0–168 integer range)
- **Sampling rate:** 120 Hz
- **Detection:** `device.deviceType == "NISO103"`

```json
{
    "deviceID":  "WA98AD87",
    "epochTime": 1777961931,
    "seqNum":    1,
    "seqPart":   1,

    "bp": {
        "bpSystolic":    122,
        "bpDiastolic":   78,
        "map":           93,
        "bpError":       0,
        "bpErrorMsg":    "No error",
        "cycleDuration": 15
    },

    "pleth": {
        "plethWave": [84, 91, 103, 118, 128, 133, 131, 124, ...]
    },

    "spo2": {
        "spo2":          99,
        "pulseRate":     77,
        "pi":            39,
        "spo2Error":     0,
        "sp2ErrorMsg":   "no error",
        "prErrorMsg":    "No error",
        "cycleDuration": 3
    },

    "device": {
        "deviceType":   "NISO103",
        "macAddress":   "c4:1a:32:de:9f:22",
        "batteryLevel": "75",
        "fwVersion":    "2.1.0",
        "hwVersion":    "1.2",
        "alarms":       []
    }
}
```

> **Send:** minimum 3000 samples, recommended **3600 samples** (30s × 120 Hz — used as-is, no resampling)

---

### Device 3 — NISO101

The NISO101 watch outputs a nested JSON structure with `device.deviceType` set to `"NISO101"`.

- **PPG field:** `pleth.plethWave` (large ADC integer values, same scale as NISO204)
- **Sampling rate:** 100 Hz
- **Detection:** `device.deviceType == "NISO101"`

```json
{
    "deviceID":  "BM-B43A45B93E15",
    "epochTime": 1777961931,
    "seqNum":    1,
    "seqPart":   1,

    "bp": {
        "bpSystolic":    118,
        "bpDiastolic":   76,
        "map":           90,
        "bpError":       0,
        "bpErrorMsg":    "No error",
        "cycleDuration": 15
    },

    "pleth": {
        "plethWave": [975533, 976179, 976685, 977206, 977736, ...]
    },

    "spo2": {
        "spo2":          98,
        "pulseRate":     74,
        "pi":            41,
        "spo2Error":     0,
        "sp2ErrorMsg":   "no error",
        "prErrorMsg":    "No error",
        "cycleDuration": 3
    },

    "device": {
        "deviceType":   "BERRYMED",
        "macAddress":   "b4:3a:45:b9:3e:15",
        "batteryLevel": "80",
        "fwVersion":    "1.0.3",
        "hwVersion":    "1.4",
        "alarms":       []
    }
}
```

> **Send:** minimum 2500 samples, recommended **3000 samples** (30s × 100 Hz → resampled to 3600 @ 120 Hz)

---

## 4. Field Reference

### 4.1 Required Fields

These fields **must** be present or the request will be rejected with HTTP 400.

| Field (Format A) | Field (Format B) | Type | Description |
|-----------------|-----------------|------|-------------|
| `BPSystolic` | `bp.bpSystolic` | `number` | Systolic blood pressure in mmHg. If device does not measure BP, send `400`. |
| `BPDiastolic` | `bp.bpDiastolic` | `number` | Diastolic blood pressure in mmHg. If device does not measure BP, send `202` `. |
| `PlethWave` or `Pleth` | `pleth.plethWave` | `array of numbers` | Raw PPG waveform. See Section 5 for signal requirements. |

---

### 4.2 Recommended Fields

These fields are not required but **significantly improve AI accuracy**.

| Field (Format A) | Field (Format B) | Type | Default | Description |
|-----------------|-----------------|------|---------|-------------|
| `Age` | — | `number` | `35` | Patient age in years |
| `Gender` | — | `string` | `"Male"` | `"Male"` or `"Female"` |
| `BMI` | — | `number` | `24.5` | Body Mass Index |
| `Height` | — | `number` | — | Height in **cm** (used to compute BMI if BMI not provided) |
| `Weight` | — | `number` | — | Weight in **kg** (used to compute BMI if BMI not provided) |
| `PRAllData` | `spo2.pulseRate` | `array of numbers` | `[75]` | Heart rate values at 1 Hz (one value per second, 30 values for 30s) |
| `Source_HZ` | — | `number` | `100` | Sampling rate of the PPG signal in Hz. Required if not 100 Hz. |

--- 

### 4.3 Optional Metadata Fields

These fields are stored and displayed but do not affect AI inference.

| Field (Format A) | Field (Format B) | Type | Description |
|-----------------|-----------------|------|-------------|
| `Name` | — | `string` | Patient name |
| `Date` | — | `string` | Date of measurement (e.g. `"2026-05-05"`) |
| — | `deviceID` | `string` | Device serial number or ID |
| — | `device.deviceType` | `string` | Device model/type name |
| — | `device.macAddress` | `string` | Device MAC address |
| — | `device.batteryLevel` | `string` | Battery level percentage |
| — | `spo2.spo2` | `number` | SpO2 percentage |
| — | `spo2.pi` | `number` | Perfusion Index |

---

### 4.4 Calibration Fields (Optional)

Send these to apply a one-time manual calibration offset. The AI output will be adjusted to match the reference values.

| Field | Type | Description |
|-------|------|-------------|
| `Reference_SBP` | `number` | Manual cuff systolic reading (mmHg) |
| `Reference_DBP` | `number` | Manual cuff diastolic reading (mmHg) |
| `Reference_Hb` | `number` | Lab hemoglobin value (g/dL) |
| `Reference_Glucose` | `number` | Lab glucose value (mg/dL) |

---

## 5. PPG Signal Requirements

> **The AI model processes at 120 Hz. It needs exactly 30 seconds of signal = 3600 samples at 120 Hz.**  
> The server resamples your device's native rate to 120 Hz automatically. You just need to send enough samples to cover 30 seconds at your device's rate.

### How many samples to send — by device

| Device | Native Rate | Send minimum | Send recommended |
|--------|-------------|-------------|-----------------|
| **CHECKME O2** | 120 Hz | 3600 samples (25s) | **3600 samples (30s)** |
| **BerryMed** | 100 Hz | 3000 samples (25s) | **3600 samples (30s)** |
| **NISO204** | 200 Hz | 3600 samples (25s) | **3600 samples (30s)** |

If fewer than the minimum samples are received, the server returns an error and no AI result is produced.

### Signal quality rules

- The waveform must show clear, continuous pulse cycles — no flat lines.
- Do not include overflow artifacts (e.g. signed 8-bit wrapping from 127 → -128). Convert to unsigned before sending.
- CHECKME value `156` is a known sentinel for finger-off — send it as-is, the server handles it.

---

## 6. Response

### 6.1 Success Response

```json
{
    "ok": true,
    "id": 5,
    "ai_result": {
        "status":   "success",
        "sbp":      118.5,
        "dbp":      74.2,
        "category": "normal",
        "hb":       12.3,
        "glucose":  98.4,
        "metadata": {
            "source_hz":       100,
            "window_seconds":  30,
            "input_samples":   3000,
            "resampled_target": 3600
        }
    }
}
```

### 6.2 AI Failed (Signal Too Short or Poor Quality)

```json
{
    "ok": true,
    "id": 6,
    "ai_result": {
        "status":  "error",
        "message": "Insufficient data. Need at least 25s (2500 samples), but have 1200."
    }
}
```

### 6.3 Request Rejected (Missing Required Fields)

**HTTP 400**
```json
{ "error": "Missing BPSystolic or BPDiastolic" }
{ "error": "Missing PlethWave or Pleth signal data" }
{ "error": "Invalid or empty JSON" }
```

### 6.4 BP Category Values

| Value | Meaning |
|-------|---------|
| `"normal"` | SBP 90–129 and DBP 60–79 mmHg |
| `"hyper"` | SBP ≥ 130 or DBP ≥ 80 mmHg |
| `"hypo"` | SBP < 90 or DBP < 60 mmHg |

---

## 7. Code Examples

### Python
```python
import requests
import json

with open("patient_data.json") as f:
    payload = json.load(f)

response = requests.post(
    "http://<server-ip>:5001/api/receive_data",
    json=payload
)

print(response.status_code)
print(response.json())
```

### curl (Linux / Git Bash)
```bash
curl -X POST http://<server-ip>:5001/api/receive_data \
     -H "Content-Type: application/json" \
     -d @patient_data.json
```

### PowerShell
```powershell
$body = Get-Content "patient_data.json" -Raw
Invoke-RestMethod -Uri "http://<server-ip>:5001/api/receive_data" `
                  -Method POST `
                  -ContentType "application/json" `
                  -Body $body
```

---

## 8. Common Mistakes

| Mistake | Result | Fix |
|---------|--------|-----|
| Sending PPG with `-100` sentinel values | AI inference fails | Strip or replace invalid samples before sending |
| Sending < 25 seconds of PPG | AI inference fails | Buffer signal until 2500+ samples collected |
| Not specifying `Source_HZ` for non-100Hz devices | Wrong resampling | Always include `Source_HZ` if rate is not 100 Hz |
| BP device reports `0/0` (not supported) | Stored as `0/0`, AI still runs | Send `0` for both fields — AI uses PPG only |
| Signed 8-bit overflow in waveform | Corrupt signal | Convert signed byte stream to unsigned before packing into JSON |
| Using curl on Windows PowerShell | Parameter error | Use `Invoke-RestMethod` instead (see Section 7) |

---

## 9. Device Compatibility Summary

| Device | JSON Format | PPG Field | Rate | Detection Key |
|--------|-------------|-----------|------|---------------|
| **NISO204** | Flat | `Pleth` (large ADC floats) | 200 Hz (`FS` field) | `DeviceName == "NISO204"` |
| **CHECKME O2** | Nested | `pleth.plethWave` (0–168 integers) | 120 Hz | `device.deviceType == "CHECKME"` or `"CHECKME_O2"` |
| **BerryMed Watch** | Nested | `pleth.plethWave` (large ADC integers) | 100 Hz | `device.deviceType == "BERRYMED"` |

> Only these three device types are currently supported. If you are integrating a new device, contact the LifeSigns team.


