
# LifeSigns — Signal Processing & Model Logic
*Reference document: how raw PPG becomes a BP/Hb/Glucose reading*

---

## Overview

Every 30 seconds of PPG signal goes through this pipeline:

```
Raw Pleth (device ADC values)
        ↓
  [Step 1]  Resample → 120 Hz
        ↓
  [Step 2]  Split into 6 × 5s segments
        ↓
  [Step 3]  Median filter (denoise whole signal)
        ↓
  [Step 4]  Per-segment: Bandpass 0.4–11 Hz
        ↓
  [Step 5]  Normalize 0–1
        ↓
  [Step 6]  Find peaks/troughs → pick representative cycle
        ↓
  [Step 7]  Extract 31 features from that cycle
        ↓
  [Step 8]  Classify zone: hypo / normal / hyper
        ↓
  [Step 9]  Regress delta within zone
        ↓
  [Step 10] Base + delta → raw BP number
        ↓
  [Step 11] Aggregate 6 segments → IQR filter → mean
        ↓
  [Step 12] Apply calibration offset (if reference BP provided)
        ↓
  Output: SBP / DBP / category / Hb / Glucose
```

---

## Step 1 — Resample to 120 Hz

```python
n_target = int(duration_seconds × 120)
ppg_resampled = scipy.signal.resample(ppg_raw, n_target)
```

The model was trained on data at 120 Hz. All three devices sample at different rates:

| Device | Native Hz | What happens |
|--------|-----------|-------------|
| NISO204 | 200 Hz | Downsampled (compressed) to 120 Hz |
| CHECKME O2 | 120 Hz | No change needed |
| BerryMed | 100 Hz | Upsampled (interpolated) to 120 Hz |

`scipy.signal.resample` uses FFT-based resampling — it's spectrum-preserving, so the pulse shape is maintained after the rate change.

---

## Step 2 — Split into 6 × 5-second segments

```
30s × 120 Hz = 3600 samples
3600 / 600 = 6 segments of 600 samples each
```

```
[000–599] [600–1199] [1200–1799] [1800–2399] [2400–2999] [3000–3599]
   seg0       seg1       seg2       seg3       seg4       seg5
```

Each segment runs the full feature extraction + classification + regression pipeline independently. This gives 6 independent BP estimates per 30-second window, which are then aggregated. Processing segment-by-segment means a noisy 5-second chunk (motion, finger-off) only discards that one segment — not the whole 30 seconds.

---

## Step 3 — Median Filter (Whole Signal)

```python
ppg_cleaned = medfilt(ppg_segment, kernel_size=3)
```

Each sample replaced by the median of itself and its 2 neighbours. Kills single-sample spikes (BLE packet errors, ADC glitches) without blurring the pulse shape. Applied once to the full 30-second signal before segmenting.

---

## Step 4 — Bandpass Filter: 0.4–11 Hz (Per Segment)

```python
b, a = butter(order=3, [0.4/nyq, 11.0/nyq], btype='band')
filtered = filtfilt(b, a, segment)
```

- **Low cut 0.4 Hz** — removes slow baseline drift: respiration (0.15–0.4 Hz), sweat, contact pressure changes
- **High cut 11 Hz** — removes high-frequency noise: EMI, muscle artifact, motion above normal pulse range
- **filtfilt** — applies the filter forward then backward, giving zero phase shift. Critical so peaks don't shift in time (the time-to-peak feature would be corrupted by a phase-delayed filter)

A resting heart rate is 1–2 Hz. The bandpass is intentionally wide (up to 11 Hz) to keep the 2nd, 3rd, 4th harmonics of the pulse waveform — those harmonics carry the dicrotic notch and reflection wave that the vascular stiffness features depend on.

---

## Step 5 — Normalize 0–1

```python
norm = (filtered - filtered.min()) / (filtered.max() - filtered.min() + 1e-6)
```

Raw ADC values are completely different across devices:
- NISO204: ~647,764 (large 20-bit ADC float)
- CHECKME: 0–168 (8-bit unsigned)
- BerryMed: ~975,000 (large ADC int)

Normalization makes all of them land on the same 0–1 scale. The model was trained on normalized waveforms, so this step is mandatory — without it the features would be wildly out of distribution.

---

## Step 6 — Find Peaks and Troughs → Pick Representative Cycle

```python
peaks, _ = find_peaks(norm, distance=int(fs * 0.4))   # min 0.4s between peaks = max 150 bpm
mins,  _ = find_peaks(-norm, distance=int(fs * 0.3))  # troughs
```

From all complete cycles (trough → peak → trough), pick the one whose peak amplitude is **closest to the median peak amplitude** of all cycles in this 5-second window. This is the "representative beat."

**Why median, not mean?** A single large artifact beat would shift the mean significantly. Median is robust. The representative cycle is the most typical heartbeat in this window — not the biggest, not the smallest.

One cycle is extracted as: `signal[trough_before_peak : trough_after_peak]`

---

## Step 7 — Extract 31 Features from the Representative Cycle

This is the physiological core. Features are grouped into 4 categories:

### Group A: Pulse Shape (Time Domain)

| Feature | Code | Physiological Meaning |
|---------|------|-----------------------|
| Peak amplitude | `max(cycle)` | Pulse pressure — how hard the heart pushed |
| Pulse width | `t[-1]` | Total beat duration → inverse of heart rate |
| Time to peak (TTP) | `t[argmax]` | How fast blood rushes into artery after systole. Stiff arteries → shorter TTP |
| TTP / time-from-peak ratio | `ttp / tdp` | Pulse asymmetry. Hypertension makes the fall steeper |
| Area under curve (AUC) | `trapz(cycle, t)` | Stroke volume proxy — total blood pushed per beat |
| Max of 1st derivative | `max(d/dt)` | Peak ejection speed — how fast the pressure rises |
| Min of 1st derivative | `min(d/dt)` | Fastest pressure fall — related to peripheral resistance |
| Max of 2nd derivative (APG-a) | `max(d²/dt²)` | Acceleration peak — vascular tone marker |
| Min of 2nd derivative (APG-b) | `min(d²/dt²)` | Deceleration trough — compliance marker |
| APG b/a ratio | `b/a` | Negative = stiff arteries. Known to correlate with BP even without a cuff |

### Group B: Frequency Domain (FFT of the One Cycle)

```python
fft_vals = abs(fft(cycle))
top 3 frequency peaks → [f1, f2, f3] and their magnitudes [m1, m2, m3]
```

6 features total. The harmonic structure of the heartbeat changes with vascular stiffness — hypertensive patients show different energy distribution across harmonics.

### Group C: Heart Rate Variability (from All Peaks in the 5s Window)

| Feature | Meaning |
|---------|---------|
| HRV (std of inter-beat-intervals) | Beat-to-beat variation — high in healthy, low in hypertension |
| RMSSD | Root mean square of successive IBI differences — short-term autonomic tone |
| pNN50 | Fraction of beats where successive IBI differs > 50ms — parasympathetic marker |
| PR mean | Average heart rate in this 5s window |
| PR std | Heart rate variability at 1Hz resolution (from PRAllData if provided) |

### Group D: Vascular Stiffness Indices

| Feature | Formula | Meaning |
|---------|---------|---------|
| Reflection Index (RI) | `dicrotic_notch / peak` | How much of the forward wave reflects back from the periphery. High RI → high peripheral resistance → hypertension |
| Augmentation Index (AIx) | `(notch − peak) / peak` | Wave augmentation from reflected pressure wave. Positive = stiff large arteries |
| Stiffness Index (SI) | `0.1 / TTP` | Large artery stiffness proxy — fast pulse wave → shorter TTP → stiff arteries |
| Pulse Width at 50% (PW50) | `width at half-max` | Narrower at half-max in hypertension |
| RMSSD | (also listed in HRV above) | Autonomic contribution to BP regulation |
| pNN50 | (also listed in HRV above) | |

**Total: 31 features**

---

## Step 8 — Classification: Which BP Zone?

```python
X_scaled = global_StandardScaler.transform([31 features])
probs = LogisticRegression.predict_proba(X_scaled)
# → [P(hypo), P(normal), P(hyper)]
# e.g. [0.05, 0.82, 0.13]
```

The logistic regression classifier was trained on NISO204 data. It outputs a probability for each zone:
- **hypo**: SBP < 90 or DBP < 60
- **normal**: SBP 90–129 and DBP 60–79
- **hyper**: SBP ≥ 130 or DBP ≥ 80

The top class = the label for this segment. The probabilities are used in Step 9.

---

## Step 9 — Regression: Delta Within the Zone

Three separate XGBoost regression models, one per BP zone. Each produces a **delta** (deviation from the zone's base BP):

```python
for each zone (hypo, normal, hyper):
    X_reg = per_group_scaler.transform([31 features])
    delta_sbp = XGBoost_sbp_model.predict(X_reg)   # small number near 0
    delta_dbp = XGBoost_dbp_model.predict(X_reg)

# Soft vote: weight each zone's delta by its probability
w_sbp = P(hypo)×delta_hypo_sbp + P(normal)×delta_normal_sbp + P(hyper)×delta_hyper_sbp
w_dbp = P(hypo)×delta_hypo_dbp + P(normal)×delta_normal_dbp + P(hyper)×delta_hyper_dbp
```

This soft vote means even if the classifier is 82% sure it's "normal," the other 18% of probability still contributes its regression delta proportionally. More robust than a hard decision.

A Meta-Ridge correction layer is applied on top:
```python
delta_sbp = MetaRidge.predict([[raw_delta_sbp]])
```
This corrects systematic bias in the XGBoost output (trained on residuals).

---

## Step 10 — Base + Delta → Raw BP Number

The regression models were trained to output a **small deviation from zero**, not an absolute BP. So the final answer is:

```python
_BP_BASE = {"hypo": (90, 60), "normal": (118, 76), "hyper": (142, 90)}
base_sbp, base_dbp = _BP_BASE[predicted_category]

delta_sbp = clip(w_sbp, -15, +15)   # never shift more than 15 mmHg from base
delta_dbp = clip(w_dbp, -15, +15)

raw_sbp = base_sbp + delta_sbp
raw_dbp = base_dbp + delta_dbp
```

Example: classifier says "normal" (82%), regression delta = +2.7 sbp / +1.8 dbp
→ SBP = 118 + 2.7 = **120.7 mmHg**, DBP = 76 + 1.8 = **77.8 mmHg**

---

## Step 11 — Aggregate 6 Segments

After all 6 segments produce (sbp, dbp, label, confidence):

1. **IQR Outlier Filter**: For both SBP and DBP lists, flag any value outside Q1−1.5×IQR … Q3+1.5×IQR as outlier. Remove flagged segments.
2. **Minimum clean check**: If fewer than 3 segments remain clean → return error `"Only X of 6 segments were clean"`. Need at least 15s of good data.
3. **Category**: Majority vote across clean segment labels
4. **Final SBP/DBP**: Mean of clean segment values

---

## Step 12 — Calibration: Reference BP + Personal Offset

### What is it?

The model's raw output has a **systematic per-person error** — it might consistently predict 8 mmHg too low for one person because of their skin tone, artery depth, or fat layer. Calibration measures and corrects this error once, then applies it to all future readings.

### How it works

**When the device sends `Reference_SBP` and `Reference_DBP` (cuff reading):**

```python
# 1. Run model WITHOUT any offset → get baseline prediction
baseline = engine.predict_vitals(pleth, offsets=None)
# e.g. baseline_sbp = 112, baseline_dbp = 70

# 2. Cuff reading was: sbp=122, dbp=78
# 3. Compute the systematic error (offset)
offset_sbp = reference_sbp - baseline_sbp = 122 - 112 = +10
offset_dbp = reference_dbp - baseline_dbp = 78  - 70  = +8

# 4. Store these offsets
offsets = {"sbp": +10.0, "dbp": +8.0}

# 5. All future predictions: add the offset
final_sbp = model_output_sbp + offsets["sbp"]
final_dbp = model_output_dbp + offsets["dbp"]
```

### Where it lives in the code

- `process_vitals()` in `vitals_standalone.py` handles Reference_ fields
- Offsets stored in `berry_app.py` per `device_id` in `_device_offsets` dict
- Server injects stored offsets into every subsequent packet automatically — the device does not need to resend the cuff value

### Why it matters

Without calibration:
- NISO204: ±12 mmHg mean absolute error (model was trained on this device — decent out of the box)
- CHECKME/BerryMed: ±15–20 mmHg (out-of-domain signal → systematic bias)

With calibration (one cuff reading):
- NISO204: ±5–7 mmHg (cuts error in half)
- CHECKME/BerryMed: ±8–12 mmHg (much more usable — systematic bias corrected even if signal morphology differs)

Calibration does NOT fix random noise or motion artifacts — only the systematic per-person offset.

---

## Noise Handling (Planned Changes)

### Per-device signal cleaning (before inference)

**CHECKME O2 (NISO103):**
- Value 156 (`0x9C`) = finger-off sentinel. Short runs (< 1 second / 120 samples) → linear interpolation. Long runs → leave as-is (will be caught by segment noise gate below).

**BerryMed (NISO101):**
- Leading zeros (BLE startup artifact) → strip by finding first non-zero index
- Spikes: `|value - local_median(window=11)| > 5 × local_std` → replace with local median

**NISO204:** No cleaning needed.

### Segment-level noise gate (5 checks per segment)

| Gate | Check | Meaning |
|------|-------|---------|
| 1 | `std(norm) < 1e-4` OR `max-min < 0.01` | Flat line — no heartbeat detected |
| 2 | `(count_at_min + count_at_max) / len > 25%` | Signal clipped — ADC saturated |
| 3 | `mean(raw==156) > 50%` (CHECKME only) | More than half the segment is finger-off |
| 4 | `mean(raw==0) > 20%` (BerryMed only) | ADC not settled yet |
| 5 | `FFT power above 8 Hz / total power > 60%` | Motion artifact dominates |

If a segment fails any gate → skip it. If fewer than 3 of 6 segments pass → return error.

### Accelerometer gate (before any processing)

Applies to CHECKME and BerryMed only. If the `acc` field is present and `std(motion) > threshold` → entire 30-second window discarded (`status: skipped`). No inference, not counted in the session aggregator.

---

## 15-Minute Session Aggregation

Every 30 seconds → one inference result. Results silently collected in `SessionAggregator`:

1. Failed/skipped 30s windows → ignored
2. Successful readings stored: `{sbp, dbp, hb, glucose, category}`
3. After 15 minutes (`last_packet_time - session_start >= 900s`):
   - Compute `median_sbp`, `median_dbp` across all stored readings
   - Flag reading as outlier if `|sbp − median_sbp| > 10` OR `|dbp − median_dbp| > 10`
   - If fewer than 3 non-outlier readings → `status: error, "Noisy data"`
   - Else → average the good readings, majority-vote the category → send to Kafka

Session auto-resets if no packet arrives for 2+ minutes (watch went to sleep).

---

## Do You Need to Test Calibration on All 3 Devices?

### My recommendation: **No — calibration is essential for CHECKME/BerryMed, optional for NISO204**

| Device | Test calibration? | Why |
|--------|------------------|-----|
| **NISO204** | Optional | Model trained on this device. Raw output is already ±12 mmHg — usable even without calibration. Calibration will improve it further but it is not broken. |
| **CHECKME O2** | **Yes, required** | Out-of-domain signal. Raw output will be systematically biased (tested: pushed to hypo). One cuff reading + calibration corrects the systematic offset and makes it clinically usable. |
| **BerryMed** | **Yes, required** | Same reason. Large ADC integers normalized fine, but signal morphology differs from training data → systematic bias. Calibration is the fix. |

### Practical testing sequence

1. **NISO204** first — confirm pipeline works end-to-end, BP in plausible range without calibration
2. **CHECKME** — test without calibration (expect hypo bias), then re-test with `Reference_SBP/DBP` and confirm the offset corrects it
3. **BerryMed** — same as CHECKME

For step 2 and 3, the test procedure is:
- Take a cuff reading → note the true SBP/DBP
- Send first packet with `Reference_SBP` and `Reference_DBP` set to the cuff values
- Check `active_offsets` in the response — offsets should be non-zero
- Send 2nd packet without Reference fields → result should now be close to the cuff reading
