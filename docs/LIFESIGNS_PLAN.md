# Plan: LifeSigns Complete Signal Processing + Alert Architecture

## Context

### Two Data Sources

**Source 1 — PPG Watch (primary measurement)**
- Devices: BerryMed (NISO101), CHECKME O2 (NISO103), NISO204
- Frequency: 30 seconds of PPG data sent every **3 minutes**
- Normal output mode: averaged result every **15 minutes** (5 readings per session)
- Format: existing JSON payloads per device type

**Source 2 — Reference BP (calibration + alert trigger)**
- Devices: NISO204, NISO206, or any BP-capable device
- Format: same nested JSON as BerryMed/CHECKME (auto-detected)
- Content: actual manually-measured BP from the patient
- Arrives: (a) at session start to establish first calibration, and (b) after each alert to recalibrate
- Delivered via a **separate Kafka topic** (not the PPG watch topic)

### System Goals
1. Per-device signal pre-processing (SG filter, sentinel cleaning, spike removal)
2. Segment-level noise gating (≥15s clean data required)
3. Accelerometer motion gate (discard 30s window if drastic movement)
4. **Reference-BP-driven calibration** — first reference BP sets the baseline; offset applied to all subsequent model outputs
5. **Drift detection + alert escalation** — detect when model output drifts significantly from calibrated baseline → alert → recalibrate → check consistency
6. **Normal mode output**: every 15 min, averaged over good readings
7. **Emergency mode output**: immediate 3-reading consensus when drift alert fires
8. Cross-device model consistency (confidence-gated fallback for non-NISO204)

---

## Architecture: Calibration + Alert Flow

### Normal Mode (no alert)

```
[Every 3 min]  Watch sends 30s PPG
                    ↓
              process_vitals() → raw_sbp, raw_dbp
                    ↓
              apply calibration offset
                    ↓
              SessionAggregator.add_reading()
                    ↓ (every 15 min, 5 readings collected)
              get_session_result() → average good readings
                    ↓
              drift_check(session_result, baseline)
                    ↓
        ┌── drift OK ──→ publish to Kafka (15-min output)
        └── drift too large → ALERT (enter escalation)
```

### Escalation Mode (after alert)

```
ALERT fired (|output - baseline| > DRIFT_THRESHOLD)
        ↓
Publish alert to Kafka:
  {"status": "alert", "reason": "BP drift detected", "last_sbp": X, "baseline_sbp": Y}
        ↓
Wait for new Reference BP from Source 2 (separate Kafka topic)
        ↓
Recalibrate: compute new offset from fresh reference reading
        ↓
Check post-recalibration drift:
  still drifting > DRIFT_THRESHOLD from NEW baseline?
        ├── No → resume normal 15-min mode (recalibration was enough)
        └── Yes → CRITICAL ALERT → enter Emergency Immediate Mode
                    ↓
          Take NEXT 3 readings (3 × 3 min = up to 9 min)
                    ↓
          Consensus check (agreement = within ±AGREE_THRESHOLD mmHg):
            ≥ 2 of 3 agree → publish IMMEDIATELY (not 15-min average)
            all 3 disagree → CRITICAL ALERT:
              {"status": "critical_alert", "reason": "Inconsistent readings — manual check required"}
```

### Thresholds

```python
AGREE_THRESHOLD  = 10   # mmHg — two readings "agree" if within this range (fixed)
MIN_GOOD_READINGS = 3   # for normal 15-min session (unchanged)
OUTLIER_THRESHOLD = 10  # mmHg median deviation for session outlier removal

# DRIFT_THRESHOLD is NOT hardcoded — it is sent in the payload.
# Field names: "drift_threshold_sbp" and "drift_threshold_dbp"
# Default fallback (if not present in payload): 20 mmHg for both
DEFAULT_DRIFT_THRESHOLD_SBP = 20
DEFAULT_DRIFT_THRESHOLD_DBP = 20
```

**Drift threshold is payload-driven:** The reference BP payload (arriving on Kafka Topic 2) may include `drift_threshold_sbp` and `drift_threshold_dbp` fields. These are set externally by a dashboard and forwarded to the server via the payload. The server stores these per device_id alongside the baseline. If the fields are absent, the default 20 mmHg is used.

```python
_device_drift_thresholds: dict[str, dict] = {}
# device_id → {"sbp": 20, "dbp": 20}  ← updated each time reference BP arrives
```

### Calibration Logic (Reference BP from Source 2)

1. **First reference BP arrives** (session start):
   - Run model on current PPG window WITHOUT offset → `baseline_model_pred`
   - `offset_sbp = reference_sbp - baseline_model_pred_sbp`
   - `offset_dbp = reference_dbp - baseline_model_pred_dbp`
   - Store `calibrated_baseline_sbp = reference_sbp` (what the patient's true BP is)
   - Store `calibrated_baseline_dbp = reference_dbp`
   - Apply offset to all subsequent readings

2. **Post-alert recalibration** (new reference BP arrives after alert):
   - Repeat same computation with new reference values
   - Update `calibrated_baseline_sbp/dbp` to the new values
   - Reset SessionAggregator (start fresh 15-min window)

3. **Offset persistence**: stored per `device_id` in `_device_offsets` dict in `berry_app.py`

### NISO206 Device

- Same JSON format as BerryMed/CHECKME (nested `pleth`, `bp`, `spo2`, `device` blocks)
- Add `"NISO206"` to the detection tuple: `dtype in ("BERRYMED", "BERRY", "NISO101", "NISO206")`
- Confirmed sampling rate and PPG field TBD from actual hardware output

---

## Confirmed Data Characteristics (from file inspection)

| Device | API Name | File | Field | Samples | Hz | Issues |
|--------|----------|------|-------|---------|-----|--------|
| NISO204 | NISO204 | Abhilasha_2024-08-24.json | `Pleth` (floats) | 6000 | 200 | Clean — no action needed |
| CHECKME O2 | NISO103 | record_2026-05-05_13-32-30.json | `pleth.plethWave` | 3826 | 120 | 38 sentinel-156 values (0x9C = -100 signed = finger-off) |
| BerryMed Watch | NISO101 | Berrymed.json | `pleth.plethWave` | 6624 | 100 | First sample = 0 (startup artifact), large ADC ints |

---

## Scope

All data arrives via **HTTP POST** to the Flask server. BLE connection, BLE packet parsing, and device scanning are **out of scope** — the containerized gateway handles that before the data reaches this server.

## Files to Modify

- `vitals_standalone.py` — device detection, pre-processing, motion gate, output format, SessionAggregator
- `inference_engine.py` — noise gate logic, segment minimum check, cross-device model adjustment
- `berry_app.py` — per-device offset cache, SessionAggregator wiring, Kafka dispatch (HTTP handler only)

---

## Input Payload Format — All 3 Devices

### Device 1 — NISO204
```json
{
    "DeviceName":  "NISO204",
    "BPSystolic":  122,
    "BPDiastolic": 78,
    "Pleth":       [647764.0, 648201.5, 649033.2, 648750.1, ...],
    "FS":          200,
    "Name":   "John Doe",
    "Age":    35,
    "Gender": "Male",
    "BMI":    24.5,
    "PRAllData": [72, 74, 73, 75, 71, 72, 74, 73, ...]
}
```
- PPG: `Pleth` — large ADC floats, **6000 samples @ 200 Hz** for 30s
- No accelerometer (stationary device)
- BP sentinel if no cuff: `BPSystolic=400, BPDiastolic=202`

---

### Device 2 — NISO103 (CHECKME O2 Max)
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

    "acc": [12, 14, 11, 13, 15, 11, 12, 13, ...],

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
        "deviceType":   "CHECKME",   /* or whatever the hardware actually sends — confirmed from real device */
        "macAddress":   "c4:1a:32:de:9f:22",
        "batteryLevel": "75",
        "fwVersion":    "2.1.0",
        "hwVersion":    "1.2",
        "alarms":       []
    }
}
```
- PPG: `pleth.plethWave` — unsigned bytes 0–168, **value 156 = 0x9C = finger-off sentinel**
- Sampling rate: **120 Hz** (user-confirmed). Exact samples per 30s packet TBD from real device — real test file had 3826 samples (~31.9s). Minimum accepted: 3000 samples (25s × 120 Hz).
- `acc`: flat array of unsigned bytes (0–255), one motion byte per PPG sample (protocol source: 9-byte BLE sample = RED 4B + IR 4B + motion 1B; by the time it reaches HTTP server, motion array is extracted separately)
- Detection: `device.deviceType == "NISO103"`
- BP sentinel if no cuff: `bp.bpSystolic=400, bp.bpDiastolic=202`

---

### Device 3 — NISO101 (BerryMed Watch)
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
        "plethWave": [0, 975533, 976179, 976685, 977206, 977736, ...]
    },

    "acc": [[0.02, -0.01, 9.81], [0.03, 0.01, 9.79], [0.01, 0.00, 9.82], ...],

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
- PPG: `pleth.plethWave` — large ADC integers (~975000–1200000), **first sample often 0 = startup artifact**
- **3000 samples @ 100 Hz** for 30s (minimum 2500)
- `acc`: list of `[x, y, z]` triplets in g-units, one per sample or at lower rate (e.g. 25 Hz)
- Detection: `device.deviceType == "BERRYMED"` or `"BERRY"`
- BP sentinel if no cuff: `bp.bpSystolic=400, bp.bpDiastolic=202`

---

## Part 1 — Device Detection (`vitals_standalone.py`)

Detection is **purely based on `device.deviceType`** from whatever the hardware sends. The "NISO103" / "NISO101" names in the API guide are human-readable labels — the actual `deviceType` strings will be confirmed from real device output.

**Current detection strings (keep as-is — confirmed from real data):**
```python
if dtype in ("CHECKME", "CHECKME_O2"):    return DEVICE_CHECKME   # NISO103 family
if dtype in ("BERRYMED", "BERRY"):        return DEVICE_BERRYMED  # NISO101 family
```

> **Do NOT add "NISO103" or "NISO101" to detection until confirmed from actual hardware output.** When confirmed, add to the respective tuple.

`_extract_pleth()` Hz: CHECKME → **120 Hz** (user-confirmed), BERRYMED → **100 Hz**.

---

## Part 2 — Device-Specific Signal Cleaning (`vitals_standalone.py`)

New function `_clean_signal(pleth, device_type)` called inside `process_vitals()` immediately after `_extract_pleth()`.

---

### NISO103 (CHECKME) — Sentinel Interpolation

**Problem:** Value 156 (0x9C) marks finger-off. Short gaps are interpolatable; long runs mean the device was truly off and should fail the segment noise gate.

**Exact algorithm:**
```python
def _clean_checkme(pleth):
    arr = np.array(pleth, dtype=float)
    sentinel = 156

    i = 0
    while i < len(arr):
        if arr[i] == sentinel:
            # Find the run end
            j = i
            while j < len(arr) and arr[j] == sentinel:
                j += 1
            run_len = j - i

            if run_len > 120:  # > 1 second at 120 Hz → leave for noise gate
                i = j
                continue

            # Linear interpolation from last valid before → first valid after
            left_val  = arr[i - 1] if i > 0 else arr[j] if j < len(arr) else 0
            right_val = arr[j]     if j < len(arr) else left_val
            for k in range(i, j):
                t = (k - i + 1) / (run_len + 1)
                arr[k] = left_val + t * (right_val - left_val)
            i = j
        else:
            i += 1
    return arr.tolist()
```

---

### NISO101 (BerryMed) — Leading Zero Strip + Spike Removal

**Problem 1 — Leading zeros:** BLE startup artifact. First N samples may be 0 before the ADC settles.

**Problem 2 — Spikes:** Large isolated deviations caused by motion or BLE packet errors.

**Exact algorithm:**
```python
def _clean_berrymed(pleth):
    arr = np.array(pleth, dtype=float)

    # Step 1: Strip leading zeros
    first_nonzero = 0
    while first_nonzero < len(arr) and arr[first_nonzero] == 0:
        first_nonzero += 1
    arr = arr[first_nonzero:]

    if len(arr) == 0:
        return pleth  # all zeros — pass as-is, will fail min_samples check

    # Step 2: Spike removal using local median filter
    WINDOW = 11        # 11-sample neighbourhood (~110ms @ 100 Hz)
    SPIKE_SIGMA = 5.0  # spike threshold: 5 × local std

    cleaned = arr.copy()
    half = WINDOW // 2
    for i in range(len(arr)):
        lo = max(0, i - half)
        hi = min(len(arr), i + half + 1)
        neighbourhood = arr[lo:hi]
        local_med = np.median(neighbourhood)
        local_std = np.std(neighbourhood)
        if local_std > 0 and abs(arr[i] - local_med) > SPIKE_SIGMA * local_std:
            cleaned[i] = local_med

    return cleaned.tolist()
```

---

### NISO204 — No cleaning needed
Raw float Pleth values are already clean. Pass through unchanged.

---

## Part 3 — Accelerometer Motion Gate (`vitals_standalone.py`)

Runs on every 30s window **before** `_clean_signal()` and any inference. Applies only to NISO103 and NISO101. NISO204 has no accelerometer.

**Field lookup order** (check top-level JSON first, then inside `device` block):
```
"acc", "accelerometer", "accel", "motion", "imu"
```
If no field found → skip gate, proceed normally.

**Exact detection function:**
```python
def _detect_motion(acc_data):
    """
    Returns True if drastic motion detected.
    Handles two formats from the two devices:
      - NISO103: flat list of unsigned bytes (0-255), one per PPG sample
      - NISO101: list of [x,y,z] triplets in g-units
    """
    if not acc_data:
        return False

    arr = np.array(acc_data, dtype=float)

    if arr.ndim == 2 and arr.shape[1] == 3:
        # [x,y,z] triplets — compute vector magnitude per sample, then std
        mag = np.linalg.norm(arr, axis=1)           # shape: (N,)
        THRESHOLD = 0.3                              # g-units std
        return float(np.std(mag)) > THRESHOLD

    if arr.ndim == 1:
        if len(arr) % 3 == 0 and len(arr) >= 6:
            # Could be flat [x,y,z,x,y,z,...] — try reshaping
            mag = np.linalg.norm(arr.reshape(-1, 3), axis=1)
            return float(np.std(mag)) > 0.3
        else:
            # Single motion byte per sample (NISO103 raw motion byte, 0-255)
            # High std = patient moving; resting std is typically < 5 units
            THRESHOLD = 15                           # raw byte units
            return float(np.std(arr)) > THRESHOLD

    return False  # unrecognised shape → don't block
```

**Integration in `process_vitals()` (before `_clean_signal`):**
```python
if device_type in (DEVICE_CHECKME, DEVICE_BERRYMED):
    acc_data = None
    for candidate in ("acc", "accelerometer", "accel", "motion", "imu"):
        acc_data = json_data.get(candidate)
        if acc_data is None:
            acc_data = (json_data.get("device") or {}).get(candidate)
        if acc_data is not None:
            break

    if acc_data is not None and _detect_motion(acc_data):
        return {
            "status":  "skipped",
            "message": "Motion detected — window discarded. Patient was moving during this 30s window."
        }
```

---

## Part 3b — Signal Processing Upgrades (`inference_engine.py`)

Three improvements to the feature extraction pipeline — no model retraining required.

---

### 3b-1: Replace Butterworth Bandpass with High-Pass + Savitzky-Golay

**Why:** For finger/ring PPG the dicrotic notch and systolic upstroke are sharp and contain harmonic energy above 11 Hz. The Butterworth low-pass at 11 Hz blurs these features before the 1st/2nd derivative (APG) calculations. SG preserves peak width, height, and area via local polynomial regression.

**Implementation:**

```python
from scipy.signal import savgol_filter, butter, filtfilt

def _highpass(self, sig):
    """0.4 Hz high-pass — removes baseline drift, replaces Butterworth low-cut."""
    nyq = 0.5 * self.fs
    b, a = butter(3, 0.4 / nyq, btype='high')
    return filtfilt(b, a, sig)

def _savgol_smooth(self, sig):
    """Savitzky-Golay smoothing — preserves peak morphology for finger PPG.
    window=11 (~92ms @ 120Hz), poly=3 preserves up to cubic pulse shape features."""
    return savgol_filter(sig, window_length=11, polyorder=3)

def _preprocess(self, sig):
    """Full pre-processing: drift removal + morphology-preserving smoothing."""
    return self._savgol_smooth(self._highpass(sig))
```

Replace every call to `self._bandpass(seg)` in `_bp_features()` and `_hb_glu_features()` with `self._preprocess(seg)`.

The old `_bandpass()` method is removed. The high-pass component stays as `_highpass()` (used separately for global peak detection in 3b-2).

---

### 3b-2: HRV Across Full 30s Window + Parabolic Peak Interpolation

**Why (two separate problems):**

**Problem A — 5s window is statistically invalid for HRV.** At 72 bpm you get ~6 beats per segment. RMSSD and pNN50 on 5 IBIs are meaningless. HRV needs minimum ~20–30 beats (≥20s at rest).

**Problem B — 120 Hz = 8.33ms resolution.** RMSSD measures millisecond IBI differences. A ±8.33ms quantization error per peak is larger than the HRV signal you're trying to measure.

**Fix — parabolic peak interpolation for sub-sample timing:**

```python
def _interpolate_peak(self, sig, idx):
    """Parabolic interpolation: fit parabola to 3 points around peak.
    Returns fractional sample index with sub-sample accuracy."""
    if idx <= 0 or idx >= len(sig) - 1:
        return float(idx)
    y0, y1, y2 = sig[idx-1], sig[idx], sig[idx+1]
    denom = 2.0 * (2*y1 - y0 - y2)
    if abs(denom) < 1e-10:
        return float(idx)
    return idx + (y2 - y0) / denom  # fractional index

def _compute_global_hrv(self, ppg_full, pr_all_data):
    """Compute HRV features from the full 30s window.
    Returns dict of HRV metrics to be shared across all segment feature vectors."""
    ppg_hp = self._highpass(ppg_full)
    norm = (ppg_hp - ppg_hp.min()) / (ppg_hp.max() - ppg_hp.min() + 1e-9)
    raw_peaks, _ = find_peaks(norm, distance=int(self.fs * 0.4))

    if len(raw_peaks) < 4:
        return None  # not enough beats for HRV

    # Parabolic interpolation → fractional sample locations
    interp_locs = np.array([self._interpolate_peak(norm, p) for p in raw_peaks])

    # IBI in milliseconds (1ms effective resolution after interpolation)
    ibi_ms = np.diff(interp_locs) / self.fs * 1000.0

    # Remove physiologically impossible IBIs (< 300ms or > 2000ms)
    ibi_ms = ibi_ms[(ibi_ms > 300) & (ibi_ms < 2000)]

    if len(ibi_ms) < 3:
        return None

    rmssd = float(np.sqrt(np.mean(np.diff(ibi_ms) ** 2)))
    pnn50 = float(np.sum(np.abs(np.diff(ibi_ms)) > 50) / max(len(ibi_ms)-1, 1))
    hrv_std = float(np.std(ibi_ms))

    # PR from PRAllData if available, else from IBI
    if pr_all_data and len(pr_all_data) >= 6:
        pr_arr = np.array(pr_all_data, dtype=float)
        pr_arr[(pr_arr == 0) | (pr_arr > 250)] = np.nan
        pr_mean = float(np.nanmean(pr_arr))
        pr_std  = float(np.nanstd(pr_arr)) if np.nanstd(pr_arr) > 0.1 else 2.0
    else:
        pr_mean = float(60000.0 / np.mean(ibi_ms))
        pr_std  = float(np.std(60000.0 / ibi_ms)) if len(ibi_ms) > 1 else 2.0

    return {
        "hrv": hrv_std,
        "rmssd": rmssd,
        "pnn50": pnn50,
        "pr_mean": pr_mean,
        "pr_std": pr_std,
    }
```

**Integration in `predict_vitals()`:** Call `_compute_global_hrv()` ONCE before the segment loop. Pass the result dict into `_bp_features()` as `hrv_override`. If `hrv_override` is not None, skip the per-segment HRV calculation and use the global values instead.

---

### 3b-3: Beat Averaging — Extract Features from ALL Valid Beats

**Why:** Currently `_bp_features()` picks one median-amplitude cycle and extracts morphological features from it alone. With 6–8 beats per 5s segment, 5–7 beats are discarded. Averaging features across all valid beats reduces random measurement noise and is well-supported in cuffless BP research.

**Beat validation rule:** A beat is valid if its peak amplitude is within 2 standard deviations of the mean peak amplitude across all beats in the segment. Beats outside this range are motion-corrupted or artifact.

**Implementation — rewrite the cycle extraction + feature averaging section of `_bp_features()`:**

```python
# OLD: pick one cycle closest to median amplitude
# c = min(cycles, key=lambda cyc: abs(np.max(cyc) - median_amp))

# NEW: extract morphological features from every valid beat, then average
peak_amps = np.array([np.max(cyc) for cyc in cycles])
mean_amp  = np.mean(peak_amps)
std_amp   = np.std(peak_amps)
BEAT_SIGMA = 2.0

valid_cycles = [
    cyc for cyc, amp in zip(cycles, peak_amps)
    if abs(amp - mean_amp) <= BEAT_SIGMA * std_amp
]

if not valid_cycles:
    return None

# Extract morphological features (Group A + D) from each valid beat
def _morph_feats_single(cyc, fs):
    """Extract Group A + D features from one beat cycle."""
    t = np.linspace(0, len(cyc) / fs, len(cyc))
    d1 = np.gradient(cyc)
    d2 = np.gradient(d1)
    ttp = t[np.argmax(cyc)]
    tdp = t[-1] - ttp if t[-1] - ttp != 0 else 1e-6
    auc = np.trapezoid(cyc, t)
    # APG
    a_w, b_w = np.max(d2), np.min(d2)
    # Vascular stiffness
    peak_idx = np.argmax(cyc)
    post_peak = cyc[peak_idx:]
    notch_mins, _ = find_peaks(-post_peak)
    ri  = float(post_peak[notch_mins[0]] / (np.max(cyc) + 1e-9)) if len(notch_mins) > 0 else 0.0
    aix = float((post_peak[notch_mins[0]] - np.max(cyc)) / (np.max(cyc) + 1e-9)) if len(notch_mins) > 0 else 0.0
    si  = float(0.1 / (ttp + 1e-9))
    above_half = np.where(cyc >= np.max(cyc) * 0.5)[0]
    pw50 = float(len(above_half) / fs) if len(above_half) > 0 else 0.0
    return [
        float(np.max(cyc)), float(t[-1]), float(ttp), float(ttp/tdp),
        float(np.max(d1)), float(np.min(d1)), float(np.max(d2)), float(np.min(d2)),
        float(a_w), float(b_w), float(b_w/(a_w+1e-9)),  # APG
        float(auc), ri, aix, si, pw50,
    ]

morph_list = [_morph_feats_single(cyc, self.fs) for cyc in valid_cycles]
morph_avg  = np.mean(morph_list, axis=0).tolist()   # averaged Group A + D features
```

The FFT features (Group B) come from the median-amplitude cycle (unchanged — FFT of an average cycle is not meaningful). The HRV features (Group C) come from `hrv_override` (global 30s computation from 3b-2). The final 31-feature vector is assembled from: `morph_avg + fft_feats + hrv_feats`.

---

## Part 4 — Enhanced Noise Detection (`inference_engine.py`)

### Rewrite `_is_noisy(self, seg_raw, seg_norm, device_type=None)`

**Two inputs:** raw segment (before normalization, for sentinel checks) + normalized segment (for signal quality checks). Currently the function only receives normalized.

**7-gate check (Gates 1–5 = physical boundary checks, Gates 6–7 = Statistical Quality Indices):**

```python
from scipy.stats import skew as scipy_skew, kurtosis as scipy_kurtosis

def _is_noisy(self, seg_raw, seg_norm, device_type=None):
    raw  = np.array(seg_raw,  dtype=float)
    norm = np.array(seg_norm, dtype=float)

    # Gate 1 — Flat signal (after normalization)
    if np.std(norm) < 1e-4 or (norm.max() - norm.min()) < 0.01:
        return True

    # Gate 2 — Clipping (after normalization)
    vmin, vmax = norm.min(), norm.max()
    clipped = np.sum((norm == vmin) | (norm == vmax))
    if clipped / len(norm) > 0.25:
        return True

    # Gate 3 — NISO103 (CHECKME) sentinel dominance (on raw values, before clean)
    if device_type == DEVICE_CHECKME:
        if np.mean(raw == 156) > 0.50:  # >50% of segment is finger-off
            return True

    # Gate 4 — NISO101 (BerryMed) zero dominance (on raw values)
    if device_type == DEVICE_BERRYMED:
        if np.mean(raw == 0) > 0.20:    # >20% zeros = ADC not settled / off wrist
            return True

    # Gate 5 — High-frequency motion artifact (after normalization)
    # Power above 8 Hz / total power > 60% → motion dominated
    fft_power  = np.abs(np.fft.rfft(norm)) ** 2
    freqs      = np.fft.rfftfreq(len(norm), d=1.0/120.0)  # model always at 120 Hz
    hf_power   = fft_power[freqs > 8].sum()
    tot_power  = fft_power.sum()
    if tot_power > 0 and hf_power / tot_power > 0.60:
        return True

    # Gate 6 — Skewness SQI (Statistical Quality Index)
    # Clean finger PPG amplitude distribution: right-skewed (most time near diastolic baseline,
    # brief systolic peak). Valid range: 0.3 < skewness < 3.5
    # Motion/flat/noise → skewness near 0 (symmetric) or negative (inverted)
    skewness = float(scipy_skew(norm))
    if skewness < 0.3 or skewness > 3.5:
        return True

    # Gate 7 — Kurtosis SQI (Statistical Quality Index)
    # Clean PPG: moderate excess kurtosis (1.5–8.0, Fisher definition)
    # Very high kurtosis (>10): isolated sharp spikes dominate distribution
    # Near-zero or negative kurtosis (<-0.5): flat/motion-smeared signal, no distinct peaks
    kurtosis_val = float(scipy_kurtosis(norm))  # Fisher definition: normal dist = 0
    if kurtosis_val < -0.5 or kurtosis_val > 10.0:
        return True

    return False
```

> **Important:** The call sites in `predict_vitals()` must be updated to pass both `seg_raw` and `seg_norm`.

### Why skewness and kurtosis catch what the other gates miss

| Gate | What it catches |
|------|----------------|
| 1 — Flatness | Completely dead signal (std ≈ 0) |
| 2 — Clipping | ADC rail saturation |
| 3 — Sentinel | CHECKME finger-off markers |
| 4 — Zero dominance | BerryMed ADC not settled |
| 5 — HF power | High-frequency motion artifact |
| **6 — Skewness** | **Waveform shape wrong: symmetric/inverted distribution = not a clean PPG pulse** |
| **7 — Kurtosis** | **Isolated large spikes (kurtosis > 10) or no distinct peaks at all (kurtosis < -0.5)** |

A signal can pass Gates 1–5 and still have the wrong shape. For example: low-amplitude sinusoidal noise (not flat, not clipped, not a sentinel, not HF-dominated) will have near-zero skewness and will fail Gate 6. This is the orthogonal coverage skewness provides.

### Threshold validation note
The thresholds (0.3–3.5 for skewness, −0.5–10.0 for kurtosis) are starting values from literature. Validate against your 3 device files before treating them as fixed:
```python
# Quick threshold check against a known-good signal:
from scipy.stats import skew, kurtosis
seg = normalized_ppg_segment  # a 600-sample clean segment at 120 Hz
print(f"skew={skew(seg):.3f}, kurtosis={kurtosis(seg):.3f}")
# Expected for clean finger PPG: skew ~0.8–1.5, kurtosis ~2–5
```

---

## Part 5 — 15-Second Minimum Gate (`inference_engine.py`)

### In `predict_vitals()`, after the segment loop:

```python
MIN_VALID_SEGMENTS = 3  # 3 × 5s = 15 seconds minimum clean data
TOTAL_SEGMENTS     = 6  # always 6 × 5s = 30s

if len(seg_predictions) < MIN_VALID_SEGMENTS:
    # Return structured error dict — vitals_standalone.py converts to error response
    return {
        "_error": True,
        "valid": len(seg_predictions),
        "total": TOTAL_SEGMENTS,
        "clean_seconds": len(seg_predictions) * 5
    }
```

In `vitals_standalone.py`, check for the `_error` key:
```python
if results and results.get("_error"):
    v, t, s = results["valid"], results["total"], results["clean_seconds"]
    return {
        "status":  "error",
        "message": f"Only {v} of {t} segments were clean ({s}s). Need at least 15s of valid signal."
    }
```

---

## Part 6 — Pass `device_type` Through the Pipeline (`vitals_standalone.py` + `inference_engine.py`)

`device_type` must flow: `_detect_device()` → `process_vitals()` → `engine.predict_vitals()` → `_is_noisy()`.

Changes:
- `process_vitals(json_data, source_hz=None)`: call `_detect_device()` at top, store as `device_type`, pass into `engine.predict_vitals(..., device_type=device_type)`
- `predict_vitals(ppg_segment, actual_rate_hz, age, gender, bmi, pr_all_data, offsets, device_type=None)`: new optional param, passed to `_is_noisy()`
- `_is_noisy(seg_raw, seg_norm, device_type=None)`: updated signature (see Part 4)
- `_extract_pleth()`: CHECKME Hz stays **120** (user spec), BERRYMED stays **100**

---

## Part 7 — Cross-Device Model Consistency (`inference_engine.py`)

**Problem:** Model trained on NISO204 → NISO103/NISO101 features land in hypo region of feature space.

**Fix (no retraining):** Confidence-gated category fallback.

In `predict_vitals()`, after the BP category vote:
```python
if device_type != DEVICE_NISO204 and not offsets:
    # Collect per-segment class probabilities across valid segments
    # Each segment voted via logistic regression; get mean probability per class
    mean_proba = np.mean([p["proba"] for p in seg_predictions], axis=0)
    dominant_conf = float(mean_proba.max())

    if category in ("hypo", "hyper") and dominant_conf < 0.65:
        category = "normal"
        # Regression delta still applied — SBP/DBP shift within ±15 mmHg of normal base
```

This means:
- **NISO204**: unchanged, full trust in classifier
- **NISO103/NISO101 with calibration** (`offsets` set): unchanged, trust calibrated output
- **NISO103/NISO101 without calibration**: low-confidence hypo/hyper → override to "normal"; delta regression still personalizes within ±15 mmHg of 118/76

---

## Part 8 — Richer Output Metadata (`vitals_standalone.py`)

The returned result dict from `process_vitals()`:
```python
{
    "status":   "success",
    "sbp":      118.5,
    "dbp":      74.2,
    "bp":       "118.5/74.2",          # NEW: display string
    "category": "normal",
    "hb":       12.3,
    "glucose":  98.4,
    "active_offsets": {},
    "metadata": {
        "device_type":      "NISO103",  # NEW
        "source_hz":        120,
        "valid_segments":   5,          # NEW
        "total_segments":   6,          # NEW
        "signal_quality":   0.83,       # NEW: valid_segments / 6
        "window_seconds":   30,
        "input_samples":    3826,
        "resampled_target": 3600
    }
}
```

Error — motion discarded:
```json
{"status": "skipped", "message": "Motion detected — window discarded."}
```

Error — too few clean segments:
```json
{"status": "error", "message": "Only 2 of 6 segments were clean (10s). Need at least 15s of valid signal."}
```

Error — not enough data sent:
```json
{"status": "error", "message": "Insufficient data. Need at least 25s (3000 samples), but have 1200."}
```

---

## Part 9 — Session Management: Calibration, Aggregation + Alert Escalation

### Watch cadence
- PPG data arrives every **3 minutes** (one 30s window per packet)
- 15-minute session = **5 readings** (5 × 3 min)
- Session gap: if no packet for > 4 minutes → new session begins

### 9a — Session Boundary Detection

```python
SESSION_GAP_S      = 240   # 4 min silence = new session (3 min cadence + 1 min buffer)
SESSION_DURATION_S = 900   # 15 min active window = 5 readings
```

Packet time source (priority): `epochTime` field from JSON → fallback `time.time()`

### 9b — Reference BP Source (Kafka Topic 2) + Offset Cache (`berry_app.py`)

Reference BP arrives on a **separate Kafka consumer topic** (not the PPG watch topic). The server maintains:

```python
_device_offsets:          dict[str, dict]  = {}  # device_id → {sbp, dbp, hb, glucose}
_device_baselines:        dict[str, dict]  = {}  # device_id → {sbp, dbp} true calibrated BP
_device_mode:             dict[str, str]   = {}  # device_id → "normal" | "escalation" | "emergency"
_emergency_buffer:        dict[str, list]  = {}  # device_id → list of last 3 readings in emergency mode
_device_drift_thresholds: dict[str, dict]  = {}  # device_id → {sbp: N, dbp: N} — from payload
```

**Calibration flow (triggered by reference BP from Topic 2):**
```python
def handle_reference_bp(device_id, ref_sbp, ref_dbp, current_ppg_data, payload):
    # 1. Run model clean (no offset) to get baseline prediction
    baseline_pred = process_vitals(current_ppg_data, offsets=None)
    
    # 2. Compute offset
    offset_sbp = ref_sbp - baseline_pred["sbp"]
    offset_dbp = ref_dbp - baseline_pred["dbp"]
    _device_offsets[device_id]   = {"sbp": offset_sbp, "dbp": offset_dbp}
    _device_baselines[device_id] = {"sbp": ref_sbp,    "dbp": ref_dbp}

    # 3. Store drift thresholds from payload (dashboard-set, forwarded via payload)
    #    Fields: "drift_threshold_sbp", "drift_threshold_dbp" — both optional
    _device_drift_thresholds[device_id] = {
        "sbp": float(payload.get("drift_threshold_sbp", DEFAULT_DRIFT_THRESHOLD_SBP)),
        "dbp": float(payload.get("drift_threshold_dbp", DEFAULT_DRIFT_THRESHOLD_DBP)),
    }

    # 4. Reset session (new calibration = fresh 15-min window)
    _session_aggregators[device_id].reset()
    _device_mode[device_id] = "normal"
```

> **Payload fields for drift thresholds (optional):**
> | Field | Type | Default | Description |
> |-------|------|---------|-------------|
> | `drift_threshold_sbp` | `number` | `20` | mmHg SBP deviation from baseline that triggers alert |
> | `drift_threshold_dbp` | `number` | `20` | mmHg DBP deviation from baseline that triggers alert |
>
> These are set from a dashboard and forwarded to the server inside the reference BP payload. If absent, 20 mmHg is used for both. Thresholds are stored per device_id and remain in effect until the next reference BP payload updates them.

### 9c — Drift Detection + Alert Escalation (`berry_app.py`)

```python
AGREE_THRESHOLD  = 10   # mmHg — two readings "agree" if within this range (fixed)

def check_drift(device_id, session_result):
    if session_result["status"] != "success":
        return
    baseline = _device_baselines.get(device_id)
    if not baseline:
        return  # no calibration yet

    # Drift thresholds come from payload, stored per device; fallback = 20 mmHg
    thresh = _device_drift_thresholds.get(device_id, {})
    drift_sbp_limit = thresh.get("sbp", DEFAULT_DRIFT_THRESHOLD_SBP)
    drift_dbp_limit = thresh.get("dbp", DEFAULT_DRIFT_THRESHOLD_DBP)

    sbp_drift = abs(session_result["sbp"] - baseline["sbp"])
    dbp_drift = abs(session_result["dbp"] - baseline["dbp"])

    if sbp_drift > drift_sbp_limit or dbp_drift > drift_dbp_limit:
        mode = _device_mode.get(device_id, "normal")

        if mode == "normal":
            # First drift detection
            send_to_kafka({"status": "alert",
                           "reason": "BP drift detected from calibrated baseline",
                           "current_sbp": session_result["sbp"],
                           "baseline_sbp": baseline["sbp"],
                           "drift_sbp": sbp_drift})
            _device_mode[device_id] = "escalation"
            # Wait for new reference BP from Topic 2 → will trigger recalibration

        elif mode == "escalation":
            # Still drifting after recalibration → critical, enter emergency mode
            send_to_kafka({"status": "critical_alert",
                           "reason": "BP still drifting after recalibration — entering emergency mode"})
            _device_mode[device_id] = "emergency"
            _emergency_buffer[device_id] = []

    else:
        # Drift resolved — resume normal mode
        _device_mode[device_id] = "normal"
        send_to_kafka(session_result)   # publish normal 15-min output
```

### 9d — Emergency Immediate Mode (`berry_app.py`)

When `_device_mode[device_id] == "emergency"`, readings are NOT added to the 15-min aggregator. Instead:

```python
def handle_emergency_reading(device_id, vitals_result):
    if vitals_result.get("status") != "success":
        return

    buf = _emergency_buffer.setdefault(device_id, [])
    buf.append(vitals_result)

    if len(buf) < 3:
        return  # wait for 3 readings (up to 9 minutes)

    # 3 readings collected — consensus check
    sbp_vals = [r["sbp"] for r in buf]
    pairs_agree = sum(
        1 for i in range(3) for j in range(i+1, 3)
        if abs(sbp_vals[i] - sbp_vals[j]) <= AGREE_THRESHOLD
    )

    if pairs_agree >= 1:   # at least 1 pair agrees = 2 of 3 readings consistent
        avg_sbp = round(np.mean(sbp_vals), 1)
        avg_dbp = round(np.mean([r["dbp"] for r in buf]), 1)
        send_to_kafka({
            "status":  "emergency_output",
            "sbp":     avg_sbp,
            "dbp":     avg_dbp,
            "bp":      f"{avg_sbp}/{avg_dbp}",
            "note":    "Immediate output — 2+ of 3 readings consistent during alert",
            "readings_used": len(buf)
        })
        _device_mode[device_id] = "normal"   # back to normal after emergency resolved
        _emergency_buffer[device_id] = []
        _session_aggregators[device_id].reset()
    else:
        # all 3 readings disagree
        send_to_kafka({
            "status": "critical_alert",
            "reason": "All 3 emergency readings inconsistent — manual check required",
            "readings": sbp_vals
        })
        _emergency_buffer[device_id] = []   # clear buffer, keep in emergency mode
```

### 9e — SessionAggregator (updated for 3-min cadence, 5 readings)

```python
class SessionAggregator:
    SESSION_GAP_S      = 240   # 4 min silence → new session
    SESSION_DURATION_S = 900   # 15 min = 5 readings at 3-min cadence
    MIN_GOOD_READINGS  = 3     # need at least 3 of 5 to publish
    OUTLIER_THRESHOLD  = 10    # mmHg median deviation

    # add_reading(), get_session_result(), reset() — unchanged from earlier design
    # EXCEPT: skip add_reading if device mode is "emergency"
```

### 9f — berry_app.py main wiring (background thread)

```python
# After inference completes for a PPG packet:
packet_time = data.get("epochTime") or time.time()
mode = _device_mode.get(device_id, "normal")

if mode == "emergency":
    handle_emergency_reading(device_id, vitals_result)
else:
    agg = _session_aggregators.setdefault(device_id, SessionAggregator())
    agg.add_reading(vitals_result, packet_time=packet_time)
    if agg.is_ready():
        session_result = agg.get_session_result()
        check_drift(device_id, session_result)
        agg.reset()

# Kafka Topic 2 consumer (reference BP):
def on_reference_bp_message(device_id, ref_sbp, ref_dbp, ppg_data):
    handle_reference_bp(device_id, ref_sbp, ref_dbp, ppg_data)

**The device never manages offsets — the server does.**

### 9c — SessionAggregator class (`vitals_standalone.py`)

```python
class SessionAggregator:
    SESSION_GAP_S      = 120   # gap > 2 min → auto-reset
    SESSION_DURATION_S = 900   # 15 min session
    MIN_GOOD_READINGS  = 3
    OUTLIER_THRESHOLD  = 10    # mmHg median deviation

    def add_reading(self, result, packet_time=None):
        now = packet_time or time.time()
        # Auto-reset if watch slept
        if self.last_packet_time and (now - self.last_packet_time) > self.SESSION_GAP_S:
            self.reset()
        self.last_packet_time = now
        if self.session_start is None:
            self.session_start = now
        # Only store successes
        if result.get("status") == "success":
            self.readings.append({
                "sbp": result["sbp"], "dbp": result["dbp"],
                "hb": result["hb"], "glucose": result["glucose"],
                "category": result["category"], "ts": now
            })

    def is_ready(self):
        return (self.session_start is not None and
                (self.last_packet_time - self.session_start) >= self.SESSION_DURATION_S)

    def get_session_result(self):
        M = len(self.readings)
        if M == 0:
            return {"status": "error", "message": "No successful readings in this session."}

        sbp_vals = [r["sbp"] for r in self.readings]
        dbp_vals = [r["dbp"] for r in self.readings]
        med_sbp  = np.median(sbp_vals)
        med_dbp  = np.median(dbp_vals)

        good = [r for r in self.readings
                if abs(r["sbp"] - med_sbp) <= self.OUTLIER_THRESHOLD
                and abs(r["dbp"] - med_dbp) <= self.OUTLIER_THRESHOLD]
        N = len(good)

        if N < self.MIN_GOOD_READINGS:
            return {
                "status":  "error",
                "message": f"Noisy data — only {N} of {M} readings were consistent. Need at least 3.",
                "session_summary": {"total_readings": M, "good_readings": N, "outliers_removed": M - N}
            }

        avg_sbp = round(np.mean([r["sbp"] for r in good]), 1)
        avg_dbp = round(np.mean([r["dbp"] for r in good]), 1)
        cats    = [r["category"] for r in good]
        category = max(set(cats), key=cats.count)  # majority vote

        return {
            "status":   "success",
            "sbp":      avg_sbp,
            "dbp":      avg_dbp,
            "bp":       f"{avg_sbp}/{avg_dbp}",
            "category": category,
            "hb":       round(np.mean([r["hb"] for r in good]), 2),
            "glucose":  round(np.mean([r["glucose"] for r in good]), 1),
            "session_summary": {
                "total_readings": M, "good_readings": N,
                "outliers_removed": M - N, "session_duration_minutes": 15
            }
        }

    def reset(self):
        self.readings     = []
        self.session_start = None
        # keep last_packet_time so next packet measures gap correctly
```

### 9d — berry_app.py wiring (background thread after inference)

```python
_session_aggregators: dict[str, SessionAggregator] = {}

# After inference completes:
packet_time = data.get("epochTime") or time.time()
agg = _session_aggregators.setdefault(device_id, SessionAggregator())
agg.add_reading(vitals_result, packet_time=packet_time)
if agg.is_ready():
    kafka_payload = agg.get_session_result()
    send_to_kafka(kafka_payload)
    agg.reset()
```

---

## Verification

1. **NISO204:** `python vitals_standalone.py Abhilasha_2024-08-24_00-08-36.json`
   - Expected: detects NISO204, signal_quality=1.0, BP in plausible range, no confidence fallback applied

2. **NISO103:** `python vitals_standalone.py record_2026-05-05_13-32-30.json`
   - Expected: detects NISO103 (via CHECKME deviceType), 156 sentinels interpolated, valid_segments ≥ 3, confidence-gated category applied

3. **NISO101:** `python vitals_standalone.py Berrymed.json`
   - Expected: detects NISO101 (via BERRYMED deviceType), leading zero stripped, valid_segments ≥ 3

4. **Noise gate test:** Manually replace 4 of 6 segments with flat arrays → expect `status: error`, `"Only 2 of 6 segments were clean"`

5. **Motion gate test:** Add `"acc": [100, 200, 50, 180, 30, ...]` (high variance) to NISO103 JSON → expect `status: skipped`

6. **Motion gate pass:** Add `"acc": [12, 12, 13, 12, 12, ...]` (flat, no motion) → expect normal inference proceeds

7. **Session test:** Call `process_vitals()` 5 times with success results, call `get_session_result()` → expect averaged output with outlier count

---

## Part 10 — HTML Visualization Updates (`signal_processing_visual.html`)

Add **3 new steps** to the existing 12-step interactive visualization. Each follows the same pattern as existing steps: sidebar entry, canvas drawing, text explanation.

---

### Step 13 — Savitzky-Golay Filter (replaces Butterworth bandpass)

**Sidebar entry:**
```
Step 13: Savitzky-Golay Smoothing
```

**Canvas animation:** Draw two overlapping waveforms — raw PPG (grey, noisy) vs SG-smoothed output (blue, smooth). Animate sliding a small window (11-sample bracket) along the raw signal, leaving the smooth curve behind. Label: "window=11 samples (~92ms @ 120Hz), poly=3".

**Explanation panel:**
- Why we switched: Butterworth low-pass at 11 Hz blurs the sharp dicrotic notch and systolic upstroke that the model uses for BP estimation.
- SG fits a local polynomial (degree 3) to each 11-sample window → smooths noise without flattening peaks.
- Result: peak height, width, and area are preserved. The derivative (APG) still has a meaningful shape.
- Paired with 0.4 Hz high-pass (Step 3 equivalent) to remove baseline drift.

---

### Step 14 — Global HRV with Parabolic Peak Interpolation

**Sidebar entry:**
```
Step 14: Global HRV (Full 30s Window)
```

**Canvas animation (two parts):**

Part A — Parabolic interpolation:
- Draw 3 sample points around a peak (idx-1, idx, idx+1) as dots.
- Draw a parabola arc through them.
- Show the true peak at a fractional position between two samples.
- Label: "Sub-sample peak location → ~1ms effective timing resolution"

Part B — IBI series:
- Draw 30s of detected peaks (30–40 dots across x-axis).
- Animate drawing IBI differences as vertical bars between peaks.
- Show RMSSD formula: √(mean of squared IBI differences).
- Label: "≥20 beats needed for valid HRV — impossible in a 5s segment"

**Explanation panel:**
- Problem 1: At 72 bpm, a 5s segment has ~6 beats → RMSSD on 5 values is statistically meaningless.
- Problem 2: 120 Hz = 8.33ms per sample. IBI differences can be smaller than one sample → severe quantization error.
- Fix: Fit parabola to 3 points at each peak → fractional sample location → ~1ms effective resolution.
- Compute HRV metrics ONCE across the full 30s window (35–40 beats) and share result with all 6 segments.
- HRV features: RMSSD, pNN50, HR std — used directly in feature vector.

---

### Step 15 — Beat Averaging (All Valid Beats)

**Sidebar entry:**
```
Step 15: Beat Averaging
```

**Canvas animation:**
- Draw 6–8 individual beat cycles (small waveforms) in a row, slightly overlapping.
- Highlight 2 in red (amplitude outliers — too far from mean).
- Animate remaining 4–6 beats morphing/merging into one averaged beat (blue outline).
- Show: `valid if |amplitude - mean| ≤ 2σ`

**Explanation panel:**
- Old approach: pick ONE beat closest to median amplitude → discard 5–7 beats of data.
- New approach: extract morphological features (peak height, width, AUC, APG a/b-wave) from EVERY valid beat, then average the feature vectors.
- Beat validity rule: amplitude within 2 standard deviations of mean peak amplitude across all beats in segment.
- Beats outside 2σ are motion-corrupted or artifact — excluded from averaging.
- Result: lower random measurement noise per feature, better generalization to different heart rates.
- FFT features (frequency domain) still come from one representative beat — averaging FFT of multiple beats is not meaningful.

---

### HTML Implementation Notes

- New steps go in the same `<ul id="steps">` sidebar list, numbered 13, 14, 15.
- Each has a corresponding `<div class="step-content" id="step-13">` etc. block.
- Canvas IDs: `canvas-sg`, `canvas-hrv`, `canvas-beat`.
- Animation functions: `drawSGFilter()`, `drawGlobalHRV()`, `drawBeatAveraging()` — follow the same requestAnimationFrame pattern as existing step canvases.
- Existing quiz: add 3 new questions covering Steps 13–15 (bring total to 18 questions).

Sample new quiz questions:
```
Q16: Why does a Savitzky-Golay filter preserve peak height better than a Butterworth low-pass?
     A) It uses a higher cutoff frequency
     B) It fits a local polynomial to each window, preserving shape analytically  ✓
     C) It removes all frequencies above 11 Hz
     D) It doubles the sampling rate

Q17: What problem does parabolic peak interpolation solve for HRV at 120 Hz?
     A) It removes motion artifacts
     B) It reduces quantization error from 8.33ms to ~1ms effective resolution  ✓
     C) It increases the sampling rate to 1000 Hz
     D) It detects missing beats

Q18: In beat averaging, which beats are excluded from the feature average?
     A) Beats with amplitude below the median
     B) Beats whose amplitude deviates more than 2 standard deviations from the mean  ✓
     C) Beats that are shorter than 0.5 seconds
     D) The first and last beats in each segment
```

---

## Part 11 — End-to-End Project Development Roadmap

Create `project_roadmap.html` — a self-contained interactive HTML page showing the complete development timeline.

---

### Phase Structure

**Phase 1 — Core Signal Processing (Current sprint)**
- Files: `vitals_standalone.py`, `inference_engine.py`
- Tasks:
  - `_clean_checkme()` sentinel interpolation
  - `_clean_berrymed()` zero strip + spike removal
  - `_detect_motion()` accelerometer gate
  - Replace `_bandpass()` with `_highpass()` + `_savgol_smooth()`
  - `_compute_global_hrv()` with parabolic interpolation
  - Beat averaging across valid cycles
  - 5-gate `_is_noisy()` rewrite
  - 15s minimum clean data gate
  - `device_type` propagated through full call chain
  - Richer output metadata (device_type, valid_segments, signal_quality, bp string)
- Test: `python vitals_standalone.py <each_device_file.json>` → verify output format, signal_quality, category

**Phase 2 — Session Management + Calibration**
- Files: `vitals_standalone.py` (SessionAggregator), `berry_app.py`
- Tasks:
  - `SessionAggregator` class: 15-min window, 5 readings, outlier removal
  - `_device_offsets` cache per device_id
  - `handle_reference_bp()` calibration function
  - Per-device offset applied in `process_vitals()`
  - `_session_aggregators` dict wiring in `berry_app.py`
- Test: simulate 5 successive `process_vitals()` calls → verify `get_session_result()` returns averaged BP

**Phase 3 — Alert + Escalation System**
- Files: `berry_app.py`
- Tasks:
  - `_device_baselines`, `_device_mode`, `_emergency_buffer` state dicts
  - `check_drift()` — DRIFT_THRESHOLD=20 mmHg → alert on first breach
  - Mode transitions: normal → escalation → emergency
  - `handle_emergency_reading()` — 3-reading consensus (AGREE_THRESHOLD=10 mmHg)
  - Kafka Topic 2 consumer for reference BP messages
  - `on_reference_bp_message()` handler
- Test: inject readings that exceed DRIFT_THRESHOLD → verify alert Kafka message; inject new reference BP → verify recalibration; inject 3 consistent emergency readings → verify `emergency_output` published

**Phase 4 — Cross-Device Model Consistency**
- Files: `inference_engine.py`
- Tasks:
  - Confidence-gated category fallback for non-NISO204 without calibration
  - `mean_proba` from per-segment class probabilities
  - `dominant_conf < 0.65` → override hypo/hyper to "normal"
  - NISO204: unchanged; calibrated non-NISO204: unchanged
- Test: run CHECKME + BerryMed data WITHOUT calibration → verify category is "normal" for low-confidence outputs; run WITH calibration → verify category passes through unchanged

**Phase 5 — Integration + API Documentation**
- Files: `API_Integration_Guide.md`, `berry_app.py` HTTP endpoint
- Tasks:
  - Confirm actual `deviceType` strings from real NISO103 + NISO101 hardware → add to detection tuples
  - Add NISO206 to detection once hardware string confirmed
  - Final HTTP endpoint output matches API guide (sbp, dbp, bp, category, hb, glucose, metadata)
  - Update API_Integration_Guide.md with accelerometer field, alert response formats
- Test: POST all 3 device JSON files to running Flask server → verify HTTP 200 with correct structure

**Phase 6 — Containerization + Kafka**
- Files: `Dockerfile`, `docker-compose.yml`, Kafka consumer config
- Tasks:
  - Docker image for Flask server (Python 3.11, scipy, xgboost dependencies)
  - Kafka Topic 1 consumer (PPG watch data) wiring
  - Kafka Topic 2 consumer (reference BP data) wiring
  - `send_to_kafka()` output publisher (15-min averages, alerts, emergency outputs)
  - Health check endpoint `/health`
- Test: `docker-compose up` → POST data → verify Kafka topic receives output messages

**Phase 7 — Documentation + Knowledge Transfer**
- Files: `SIGNAL_PROCESSING_LOGIC.md` (update), `signal_processing_visual.html` (update with Steps 13–15), `project_roadmap.html`
- Tasks:
  - Add SG filter, global HRV, beat averaging to `SIGNAL_PROCESSING_LOGIC.md`
  - Add Steps 13–15 to HTML visualization (per Part 10 plan)
  - Add 3 quiz questions covering new steps
  - Final `project_roadmap.html` interactive timeline

---

### Roadmap HTML Design

`project_roadmap.html` — self-contained, no external dependencies.

**Layout:**
- Header: "LifeSigns Vitals Server — Development Roadmap"
- Horizontal phase timeline bar at top (Phase 1–7 clickable pills)
- Clicking a phase expands its task list below
- Each task has: checkbox (visual only, stored in localStorage), file tag (which file it touches), status badge (TODO / IN PROGRESS / DONE)
- Color coding: Phase 1 = blue (signal), Phase 2 = green (session), Phase 3 = orange (alerts), Phase 4 = purple (model), Phase 5 = teal (API), Phase 6 = grey (infra), Phase 7 = yellow (docs)
- Progress bar per phase: "X of Y tasks complete"
- Bottom summary: overall progress (total tasks done / total tasks)

**Persistence:** `localStorage` saves checkbox state by task ID so refreshing the page keeps progress.

---

## Verification (Updated)

1. **NISO204:** `python vitals_standalone.py Abhilasha_2024-08-24_00-08-36.json`
   - Expected: device_type="NISO204", signal_quality=1.0, BP in range, category passes through unchanged

2. **NISO103:** `python vitals_standalone.py record_2026-05-05_13-32-30.json`
   - Expected: CHECKME detected, 156 sentinels interpolated, valid_segments ≥ 3, confidence-gated if no calibration

3. **NISO101:** `python vitals_standalone.py Berrymed.json`
   - Expected: BERRYMED detected, leading zero stripped, spike removal applied, valid_segments ≥ 3

4. **Noise gate:** Inject 4 flat segments → `status: error, "Only 2 of 6 clean"`

5. **Motion gate trigger:** `"acc": [100, 200, 50, 180]` → `status: skipped`

6. **Motion gate pass:** `"acc": [12, 12, 13, 12]` → normal inference

7. **Session aggregation:** 5 successive calls → `get_session_result()` → averaged BP, outlier count

8. **Drift alert:** 2 session results > DRIFT_THRESHOLD from baseline → Kafka `status: alert`

9. **Emergency consensus:** 3 readings within AGREE_THRESHOLD → Kafka `status: emergency_output`

10. **Emergency failure:** 3 readings all outside AGREE_THRESHOLD → Kafka `status: critical_alert`

11. **HTML Steps 13–15:** Open `signal_processing_visual.html` → click Steps 13, 14, 15 → animations render, quiz questions 16–18 appear and score correctly

12. **Roadmap:** Open `project_roadmap.html` → all 7 phases visible, checkboxes persist on refresh
