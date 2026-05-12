# BERRYMED (NISO101) — Signal Processing Documentation

## 1. Device Overview

| Property | Value |
|----------|-------|
| Device Name | BerryMed / NISO101 |
| Protocol Identifier | `BERRYMED` / `NISO101` / `BERRY` |
| Sampling Rate | **100 Hz** (fixed) |
| PPG Value Range | Large ADC integers (no fixed upper bound) |
| Pleth Field Path | `pleth.plethWave` |
| Device-Embedded BP | None (ignored) |
| Accelerometer Support | Optional (currently disabled in inference) |
| Buffering | None — every packet processed immediately |

---

## 2. Payload Detection

Device type is identified via (`vitals_standalone.py`):

1. `device.deviceType` contains `"BERRY"`, `"BERRYMED"`, or `"NISO101"` (case-insensitive)
2. Fallback: payload has `pleth` key AND has `acc` field AND lacks `bp` block

```python
if "BERRY" in dtype or "BERRYMED" in dtype or "NISO101" in dtype:
    return DEVICE_BERRYMED
# Fallback heuristic:
if json_data.get("pleth") and json_data.get("acc") and not json_data.get("bp"):
    return DEVICE_BERRYMED
```

Pleth extraction:
```python
nested = json_data.get("pleth", {}) or {}
pleth  = nested.get("plethWave") or nested.get("pleth", [])
# → returned with fs = 100
```

The `bp` block in BERRYMED payloads is **intentionally ignored** — it contains no clinically usable embedded BP.

---

## 3. Preprocessing Pipeline

### 3.1 BerryMed-Specific Cleaning — `_clean_berrymed()`

Unlike CHECKME (which has a sentinel value), BERRYMED has two common artifacts:

**3.1.1 Leading Zero Strip**

ADC startup produces a run of zero samples at the start of the array. These are stripped:
```python
# Strip leading zeros (ADC startup artifact)
start = 0
while start < len(arr) and arr[start] == 0:
    start += 1
arr = arr[start:]
```

**3.1.2 Spike Removal via Local Median Filter**

Parameters:
- Window size: **11 samples** (~110 ms @ 100 Hz)
- Threshold: **5σ** (5 standard deviations from local median)

Algorithm:
```python
WINDOW = 11
SPIKE_SIGMA = 5.0
for i in range(len(arr)):
    local = arr[max(0, i-WINDOW//2) : i+WINDOW//2+1]
    med   = median(local)
    std   = std(local)
    if abs(arr[i] - med) > SPIKE_SIGMA * std:
        arr[i] = med  # Replace spike with local median
```

This removes transient electrical spikes without distorting the PPG morphology.

---

### 3.2 High-Pass Filtering — `_highpass()`

- **Type:** Butterworth, order 3
- **Cutoff:** 0.4 Hz (`0.4 / (0.5 × fs)`)
- **Purpose:** Remove baseline wander and DC offset from ADC drift

---

### 3.3 Savitzky-Golay Smoothing — `_savgol_smooth()`

- **Window:** 11 samples (~110 ms @ 100 Hz)
- **Polynomial order:** 3 (cubic)
- **Purpose:** Smooth noise while preserving PPG pulse shape (systolic peak, dicrotic notch)

Full preprocessing chain:
```python
def _preprocess(sig):
    return _savgol_smooth(_highpass(sig))
```

---

### 3.4 Segmentation

| Parameter | Value |
|-----------|-------|
| Minimum input | 25 s = **2500 samples** @ 100 Hz |
| Processing window | Last 30 s = **3000 samples** |
| BP inference segments | **6 × 5 s** (500 samples each) |
| Hb/Glucose | Single feature set from full 30 s window |

---

## 4. Noise / Motion Gating

### 4.1 Seven-Gate Noise Detector — `_is_noisy()`

Applied per 5-second segment. Any gate triggers rejection of that segment.

| Gate | Condition | Threshold |
|------|-----------|-----------|
| 1 | Flat signal | `std < 1e-4` OR `range < 0.01` |
| 2 | ADC clipping | `> 25%` samples at min or max |
| 3 | Sentinel dominance | `mean(samples == 156) > 0.50` ← CHECKME only, not applicable |
| 4 | Zero dominance | `mean(samples == 0) > 0.20` |
| 5 | High-frequency motion | `> 60%` power above 8 Hz |
| 6 | Extreme skewness | `skew < -2.0` OR `skew > 5.0` |
| 7 | Extreme kurtosis | `kurt < -1.5` OR `kurt > 15.0` |

Gate 4 (zero dominance) is the most BERRYMED-relevant — catches cases where the leading-zero strip was insufficient or the device lost contact.

Minimum valid segments required: **3 of 6** (≥ 15 seconds of clean signal).

---

### 4.2 Accelerometer Motion Gating

**Currently disabled in inference** — motion mask defaults to `[False] × 6` (all segments pass).

When re-enabled, the parameters are:

- **Motion threshold:** `MOTION_THRESH_BERRYMED = 0.3` (g-units std)
- **Axis format:** 3-axis `[[x, y, z], ...]` — Euclidean magnitude computed per sample
- **Motion score per segment:** `std(magnitude - 1.0g)` — deviation from gravity baseline
- **Trigger:** if `std > 0.3g` → segment flagged as motion-contaminated

Dictionary key lookup (when payload uses dict format):
```
"x-axis" → "x_axis" → "x"
"y-axis" → "y_axis" → "y"
"z-axis" → "z_axis" → "z"
```

If all 6 segments are motion-flagged → entire window discarded with status `"skipped"`.

---

## 5. Feature Extraction

Feature extraction for BERRYMED is identical to CHECKME. The same 31-feature BP vector and 27-feature Hb/Glucose vector are computed — device differences are handled upstream (cleaning, segmentation window size, sampling rate normalization).

### 5.1 Beat Detection

```python
peaks, _ = find_peaks(norm, distance=int(fs × 0.4))  # min 40 samples @ 100 Hz → max ~150 bpm
mins,  _ = find_peaks(-norm, distance=int(fs × 0.3)) # min 30 samples
```

Sub-sample peak timing via parabolic interpolation for ~1 ms HRV precision.

One beat cycle = trough-before-peak to trough-after-peak. Requires ≥ 2 peaks and ≥ 2 troughs.

---

### 5.2 Blood Pressure Features — 31-Feature Vector

Input normalized to [0, 1] range per segment before computation.

| Index | Feature | Description |
|-------|---------|-------------|
| 0 | Peak amplitude | Normalized peak value |
| 1 | Pulse width | Width of beat at base |
| 2 | Time-to-peak (TTP) | Samples from trough to systolic peak |
| 3 | TTP/TDP ratio | Systolic-to-diastolic timing ratio |
| 4 | max(d1) | Max of 1st derivative |
| 5 | min(d1) | Min of 1st derivative |
| 6 | max(d2) | Max of 2nd derivative (APG) |
| 7 | min(d2) | Min of 2nd derivative (APG) |
| 8 | APG_a | 1st positive APG peak |
| 9 | APG_b | 1st negative APG peak |
| 10 | APG b/a ratio | Reflects arterial stiffness |
| 11 | AUC | Area under normalized beat |
| 12 | Reflection Index | Post-peak notch relative height |
| 13 | Augmentation Index | Dicrotic notch prominence |
| 14 | Stiffness Index | `1 / TTP` proxy |
| 15 | Pulse width @ 50% | Samples above half-maximum / fs |
| 16–21 | FFT top 3 freq/mag pairs | Dominant cardiac harmonics and power |
| 22 | HRV std | `std(IBI_ms)` across beats |
| 23 | Signal mean | Mean of normalized signal |
| 24 | Signal std | Std of normalized signal |
| 25 | Signal max | Max of normalized signal |
| 26 | Signal min | Min of normalized signal |
| 27 | PR mean | Mean pulse rate (bpm) |
| 28 | PR std | Std of pulse rate |
| 29 | RMSSD | Root mean square successive IBI differences |
| 30 | pNN50 | Percentage of IBIs > 50 ms apart |

---

### 5.3 Hemoglobin / Glucose Features — 27-Feature Vector

Computed from the full 30 s window (one feature set per session).

| Index | Feature | Notes |
|-------|---------|-------|
| 0 | Peak (log1p) | From median-amplitude beat |
| 1–3 | Pulse width, TTP, TTP/TDP | |
| 4–7 | Derivative extremes (d1, d2) | |
| 8 | AUC (log1p) | |
| 9–14 | FFT top 3 freq/mag pairs | Interleaved |
| 15 | HRV std | |
| 16–19 | Normalized signal stats (log1p) | mean, std, max, min |
| 20 | AC/DC ratio (log1p) | `(max - min) / mean(|signal|)` |
| 21 | Signal entropy | `-sum(p × log(p))` |
| 22 | Signal skewness | |
| 23 | Perfusion Index (log1p) | `(max - min) / mean(signal)` |
| 24 | Signal Quality Index (log1p) | `max(beat) / std(signal)` |
| 25 | Cycle skewness | From median beat |
| 26 | Cycle kurtosis | From median beat |

**Log1p transforms** at indices `[0, 8, 16, 17, 18, 19, 20, 23]` ensure scale-invariance despite BERRYMED's large raw ADC integer range.

Normalized signal (`_pf_norm`) used for indices 16–19:
```python
_pf_norm = (ppg_filt - ppg_filt.min()) / (ppg_filt.max() - ppg_filt.min() + 1e-6)
```

---

## 6. Inference

### 6.1 Model Architecture

Same model set as CHECKME:

| Component | Type | Purpose |
|-----------|------|---------|
| `bp_classifier` | Logistic Regression | 3-class: hypo / normal / hyper |
| `bp_global_scaler` | StandardScaler | Scales 31-feature vector |
| `bp_[hypo/normal/hyper]_sbp/dbp` | Regressors | BP prediction per class |
| `bp_scaler_[hypo/normal/hyper]` | StandardScaler | Per-class feature scaling |
| `bp_[class]_[sbp/dbp]_meta` | Meta-regressor | Optional refinement layer |
| `hb_model` + `hb_scaler` | Regressor + scaler | Hemoglobin prediction |
| `glucose_model` + `glucose_scaler` | Regressor + scaler | Glucose prediction |

---

### 6.2 BP Inference Steps

**Step 1 — Resample to model rate**
BERRYMED @ 100 Hz, model @ 120 Hz → resampled:
```python
n_target = int(round(duration_s × 120))
ppg_segment = resample(ppg_segment, n_target)
```

**Step 2 — Per-segment probability-weighted regression**

For each of the 6 segments (5 s each):
```
features (31) → global_scaler → classifier → [p_hypo, p_normal, p_hyper]
for each class c:
    X_reg = scaler_c.transform(features)
    sbp_c = regressor_c_sbp.predict(X_reg)
    dbp_c = regressor_c_dbp.predict(X_reg)
weighted_sbp += p_c × sbp_c
weighted_dbp += p_c × dbp_c
```

**Step 3 — Outlier filtering**
IQR filter across all valid segment predictions. Fallback to all predictions if < 2 inliers.

**Step 4 — Category selection**
Most frequent label across clean segments.

**Step 5 — Confidence-gated fallback (shared with CHECKME)**
```python
if device_type != NISO204 and not offsets:
    if category in ("hypo", "hyper") and dominant_confidence < 0.65:
        category = "normal"
```
Without reference BP calibration, low-confidence extreme predictions default to `"normal"`.

**Step 6 — Base + delta output**

| Category | Base SBP | Base DBP |
|----------|----------|----------|
| hypo | 90.0 | 60.0 |
| normal | 118.0 | 76.0 |
| hyper | 142.0 | 90.0 |

```python
delta_sbp = clip(mean(clean_sbp_predictions), -15, +15)
delta_dbp = clip(mean(clean_dbp_predictions), -15, +15)
final_sbp  = clip(base_sbp + delta_sbp + offset_sbp, 70, 220)
final_dbp  = clip(base_dbp + delta_dbp + offset_dbp, 40, 130)
```

---

### 6.3 Hemoglobin & Glucose Inference

27 PPG features + 6 demographic features (age, age², age>60 flag, gender, age×gender, BMI) → scaler → model → output.

| Output | Range | Units |
|--------|-------|-------|
| Hb | Unclamped | g/dL |
| Glucose | Clamped [40, 400] | mg/dL |

Both suppressed (set to `None`) if BP inference fails.

---

## 7. Reference BP Calibration

BERRYMED has no built-in cuff — relies entirely on LS06 external reference.

**State machine:**

| State | Meaning |
|-------|---------|
| `no_reference` | No LS06 reading yet — raw AI output used |
| `unconfirmed` | First LS06 received, waiting for second to confirm |
| `normal` | Confirmed — offset applied to every reading |
| `breach_pending` | Session average drifted > 10 mmHg from baseline |
| `case2_pending` | 5 post-breach readings failed to match — escalated |

**Offset calculation (set once on confirmation):**
```python
offset_sbp = ls06_sbp - latest_ai_sbp
offset_dbp = ls06_dbp - latest_ai_dbp
```

Applied in inference:
```python
final_sbp += offsets.get("sbp", 0.0)
final_dbp += offsets.get("dbp", 0.0)
```

---

## 8. Test-Mode Correction (berry_app.py only)

Applied only in the local test server — NOT in production:

```python
# SBP < 100: add 15–20 mmHg
if cal_sbp < 100:
    cal_sbp += random.randint(15, 20)

# DBP < 60: add 10–15 mmHg
if cal_dbp < 60:
    cal_dbp += random.randint(10, 15)
```

---

## 9. Output Fields

```python
{
    "status":   "success",
    "sbp":      float,        # mmHg, clipped [70, 220]
    "dbp":      float,        # mmHg, clipped [40, 130]
    "category": str,          # "hypo" | "normal" | "hyper"
    "hb":       float | None, # g/dL
    "glucose":  float | None, # mg/dL, clamped [40, 400]
    "metadata": {
        "device_type":     "BERRYMED",
        "source_hz":       100,
        "valid_segments":  int,   # clean 5s segments used (max 6)
        "total_segments":  6,
        "signal_quality":  float, # valid/total
        "window_seconds":  30,
        "input_samples":   int,
    }
}
```

**Error cases:**

| Condition | Status | Message |
|-----------|--------|---------|
| < 2500 samples | `error` | Insufficient data. Need at least 25s. |
| < 3 clean segments | `error` | Only N of 6 segments were clean. Need ≥15s. |
| All segments have motion | `skipped` | Motion detected in all 6 segments. |

---

## 10. BERRYMED vs Other Devices — Key Differences

| Aspect | BERRYMED | CHECKME | NISO204 |
|--------|----------|---------|---------|
| Sampling rate | **100 Hz** | 120 Hz | Variable |
| ADC range | Large ints | 0–168 | Large floats |
| Pleth path | `pleth.plethWave` | `pleth.plethWave` | `Pleth` (top-level) |
| Cleaning | Leading zeros + spike filter | Sentinel 156 interpolation | Spike filter only |
| Sentinel value | None | 156 (finger-off) | None |
| Device BP | None | None | Yes (mismatch check) |
| Buffering | None | None | 3 packets combined |
| Motion threshold | 0.3 g std | 15 byte std | N/A |
| Motion gating | Defined but currently disabled | Optional | N/A |
| Confidence fallback | Yes (< 65% → normal) | Yes (< 65% → normal) | No |
| Resampling needed | Yes (100 → 120 Hz) | No | Depends on source Hz |
