# CHECKME (NISO103) — Signal Processing Documentation

## 1. Device Overview

| Property | Value |
|----------|-------|
| Device Name | CHECKME O2 / NISO103 |
| Protocol Identifier | `CHECKME` / `NISO103` |
| Sampling Rate | **120 Hz** (fixed) |
| PPG Value Range | **0 – 168** (8-bit ADC) |
| Pleth Field Path | `pleth.plethWave` |
| Device-Embedded BP | None (ignored) |
| Accelerometer Support | Optional |
| Buffering | None — every packet processed immediately |

---

## 2. Payload Detection

Device type is identified via (`vitals_standalone.py`):

1. `device.deviceType` contains `"CHECKME"` or `"NISO103"` (case-insensitive)
2. Fallback: payload has `bp` or `spo2` fields AND lacks `acc` field

```python
if "CHECKME" in dtype or "NISO103" in dtype:
    return DEVICE_CHECKME
# Fallback heuristic:
if json_data.get("bp") or (json_data.get("spo2") and not json_data.get("acc")):
    return DEVICE_CHECKME
```

Pleth extraction:
```python
nested = json_data.get("pleth", {}) or {}
pleth  = nested.get("plethWave") or nested.get("pleth", [])
# → returned with fs = 120
```

---

## 3. Preprocessing Pipeline

### 3.1 Sentinel Value Cleaning — `_clean_checkme()`

CHECKME hardware encodes **finger-off** as byte value **156** (0x9C). These samples must be corrected before any processing.

**Algorithm:**
1. Detect consecutive runs of value `156`
2. **Short runs (≤ 120 samples = ≤ 1 second @ 120 Hz):** linear interpolation between boundary values
   ```
   arr[k] = left_val + t × (right_val - left_val)
   where t = (k - i + 1) / (run_len + 1)
   ```
3. **Long runs (> 120 samples):** left in place — the downstream noise gate will reject these segments

Edge cases: if no left/right neighbor exists, the available boundary value is repeated.

---

### 3.2 High-Pass Filtering — `_highpass()`

- **Type:** Butterworth, order 3
- **Cutoff:** 0.4 Hz (`0.4 / (0.5 × fs)`)
- **Purpose:** Remove baseline wander and DC offset

---

### 3.3 Savitzky-Golay Smoothing — `_savgol_smooth()`

- **Window:** 11 samples (~92 ms @ 120 Hz)
- **Polynomial order:** 3 (cubic)
- **Purpose:** Suppress noise while preserving morphological features (peaks, dicrotic notch, inflection points)

Full preprocessing chain applied per segment:
```python
def _preprocess(sig):
    return _savgol_smooth(_highpass(sig))
```

---

### 3.4 Segmentation

| Parameter | Value |
|-----------|-------|
| Minimum input | 25 s = **3000 samples** |
| Processing window | Last 30 s = **3600 samples** |
| BP inference segments | **6 × 5 s** (600 samples each) |
| Hb/Glucose | Single feature set from full 30 s window |

---

## 4. Noise / Motion Gating

### 4.1 Seven-Gate Noise Detector — `_is_noisy()`

Applied per 5-second segment. Any gate triggers rejection of that segment.

| Gate | Condition | Threshold |
|------|-----------|-----------|
| 1 | Flat signal | `std < 1e-4` OR `range < 0.01` |
| 2 | ADC clipping | `> 25%` samples at min or max |
| 3 | **Sentinel dominance** | `mean(samples == 156) > 0.50` ← CHECKME-specific |
| 4 | Zero dominance | `mean(samples == 0) > 0.20` |
| 5 | High-frequency motion | `> 60%` power above 8 Hz |
| 6 | Extreme skewness | `skew < -2.0` OR `skew > 5.0` |
| 7 | Extreme kurtosis | `kurt < -1.5` OR `kurt > 15.0` |

Gate 3 is specific to CHECKME — rejects any segment where more than half the samples are the finger-off sentinel.

Minimum valid segments required for a usable inference result: **3 of 6** (≥ 15 seconds of clean signal).

---

### 4.2 Accelerometer Motion Gating — `_motion_mask_per_segment()`

Optional — only applied if `acc` data is present in the payload.

**Motion threshold:** `MOTION_THRESH_CHECKME = 15` (raw byte units std per segment)

Accelerometer field lookup order:
```
"acc" → "accelerometer" → "accel" → "motion" → "imu"
also checked under: json_data["device"][<key>]
```

Dict format support (`x-axis`/`x_axis`/`x` keys) — 3-axis vectors combined as Euclidean magnitude.

Per-segment motion score: `std(segment_samples)`

If **all 6 segments** are motion-contaminated → entire window discarded with status `"skipped"`.

---

## 5. Feature Extraction

### 5.1 Beat Detection

```python
peaks, _ = find_peaks(norm, distance=int(fs × 0.4))  # min 48 samples @ 120 Hz → max ~150 bpm
mins,  _ = find_peaks(-norm, distance=int(fs × 0.3)) # min 36 samples
```

Sub-sample peak timing via parabolic interpolation for ~1 ms HRV precision:
```python
offset = (y2 - y0) / (2 × (2×y1 - y0 - y2))
refined_peak = idx + offset
```

One beat cycle = trough-before-peak to trough-after-peak. Requires ≥ 2 peaks and ≥ 2 troughs.

Beat averaging: beats within 2σ of mean amplitude are used; others discarded as outliers.

---

### 5.2 Blood Pressure Features — 31-Feature Vector (`_bp_features()`)

Input per segment: normalized to [0, 1] range before feature computation.

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
| 16–17 | FFT top frequencies (×2) | Dominant cardiac harmonics |
| 18–19 | FFT top magnitudes (×2) | Spectral power at dominant freqs |
| 20 | FFT 3rd frequency | |
| 21 | FFT 3rd magnitude | |
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

### 5.3 Hemoglobin / Glucose Features — 27-Feature Vector (`_hb_glu_features()`)

Computed from the full 30 s window (one set per session).

| Index | Feature | Notes |
|-------|---------|-------|
| 0 | Peak (log1p) | From median-amplitude beat |
| 1 | Pulse width | |
| 2 | TTP | |
| 3 | TTP/TDP | |
| 4–7 | Derivatives (d1, d2 extremes) | |
| 8 | AUC (log1p) | Integral under cycle |
| 9–14 | FFT top 3 freq/mag pairs | Interleaved: freq0, mag0, freq1, mag1, freq2, mag2 |
| 15 | HRV std | |
| 16 | Signal mean (log1p) | Normalized signal |
| 17 | Signal std (log1p) | Normalized signal |
| 18 | Signal max (log1p) | Normalized signal |
| 19 | Signal min (log1p) | Normalized signal |
| 20 | AC/DC ratio (log1p) | `(max - min) / mean(|signal|)` |
| 21 | Signal entropy | `-sum(p × log(p))` |
| 22 | Signal skewness | |
| 23 | Perfusion Index (log1p) | `(max - min) / mean(signal)` |
| 24 | Signal Quality Index (log1p) | `max(beat) / std(signal)` |
| 25 | Cycle skewness | From median beat |
| 26 | Cycle kurtosis | From median beat |

**Log1p transforms** applied at indices `[0, 8, 16, 17, 18, 19, 20, 23]` — ensures scale-invariance across devices with different ADC ranges (CHECKME 0–168 vs NISO204 large floats).

Normalized signal (`_pf_norm`) used for indices 16–19:
```python
_pf_norm = (ppg_filt - ppg_filt.min()) / (ppg_filt.max() - ppg_filt.min() + 1e-6)
```

---

## 6. Inference

### 6.1 Model Architecture

| Component | Type | Purpose |
|-----------|------|---------|
| `bp_classifier` | Logistic Regression | 3-class: hypo / normal / hyper |
| `bp_global_scaler` | StandardScaler | Scales 31-feature vector |
| `bp_[hypo/normal/hyper]_sbp` | Regressor | SBP prediction per class |
| `bp_[hypo/normal/hyper]_dbp` | Regressor | DBP prediction per class |
| `bp_scaler_[hypo/normal/hyper]` | StandardScaler | Per-class feature scaling |
| `bp_[class]_[sbp/dbp]_meta` | Meta-regressor | Optional refinement layer |
| `hb_model` + `hb_scaler` | Regressor + scaler | Hemoglobin prediction |
| `glucose_model` + `glucose_scaler` | Regressor + scaler | Glucose prediction |

---

### 6.2 BP Inference Steps

**Step 1 — Resample to model rate (120 Hz)**
CHECKME is natively 120 Hz — no resampling needed.

**Step 2 — Per-segment probability-weighted regression**

For each of the 6 segments (5 s each):
```
features (31) → global_scaler → classifier → [p_hypo, p_normal, p_hyper]
for each class c:
    X_reg = scaler_c.transform(features)
    sbp_c = regressor_c_sbp.predict(X_reg)
    dbp_c = regressor_c_dbp.predict(X_reg)
    (optionally refined by meta-model)
weighted_sbp += p_c × sbp_c
weighted_dbp += p_c × dbp_c
```

**Step 3 — Outlier filtering**
IQR filter applied across all valid segment predictions. If fewer than 2 inliers remain, all predictions are used.

**Step 4 — Category selection**
Most frequent label across clean segments wins.

**Step 5 — Confidence-gated fallback (CHECKME-specific)**
```python
if device_type != NISO204 and not offsets:
    if category in ("hypo", "hyper") and dominant_confidence < 0.65:
        category = "normal"
```
Without a reference BP calibration, low-confidence extreme-category predictions default to `"normal"` to avoid false alarms. NISO204 is exempt because it has a built-in cuff reference.

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

Both are suppressed (set to `None`) if BP inference fails (no valid BP = no reliable perfusion signal).

---

## 7. Reference BP Calibration

CHECKME does not have a built-in cuff — it relies entirely on the LS06 external reference.

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

**Applied in inference:**
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
        "device_type":     "CHECKME",
        "source_hz":       120,
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
| < 3000 samples | `error` | Insufficient data. Need at least 25s. |
| < 3 clean segments | `error` | Only N of 6 segments were clean. Need ≥15s. |
| All segments have motion | `skipped` | Motion detected in all 6 segments. |

---

## 10. CHECKME vs Other Devices — Key Differences

| Aspect | CHECKME | BERRYMED | NISO204 |
|--------|---------|----------|---------|
| Sampling rate | 120 Hz | 100 Hz | Variable |
| ADC range | 0–168 | Large ints | Large floats |
| Pleth path | `pleth.plethWave` | `pleth.plethWave` | `Pleth` (top-level) |
| Sentinel cleaning | Value 156 interpolation | Leading zeros + spike filter | Spike filter only |
| Device BP | None | None | Yes (mismatch check) |
| Buffering | None | None | 3 packets combined |
| Motion threshold | 15 (byte std) | 0.3 g std | N/A |
| Confidence fallback | Yes (< 65% → normal) | Yes (< 65% → normal) | No |
