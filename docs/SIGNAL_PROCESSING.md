# Signal Processing Documentation

## Overview

The Estimation Project uses advanced signal processing techniques to extract meaningful physiological features from PPG (Photoplethysmography) signals. PPG measures blood volume changes in tissue using light absorption, providing non-invasive access to heart rate, vascular dynamics, and tissue perfusion.

This document covers:
- Signal conditioning and filtering
- Cycle detection and segmentation
- Feature extraction from time and frequency domains
- Quality assessment and noise handling

---

## 1. PPG Signal Characteristics

### 1.1 What is PPG?

PPG is a simple optical technique:
- **Light Source**: Red or infrared LED illuminates skin
- **Detector**: Photodiode measures reflected/transmitted light
- **Measurement**: Light absorption varies with blood volume changes
- **Result**: AC (pulsatile) + DC (static) components encode heart rate and vascular properties

### 1.2 Signal Components

```
PPG = DC + AC
     = [static baseline] + [cardiac pulsation]

AC wave morphology correlates with:
  - Arterial stiffness (vascular compliance)
  - Blood pressure
  - Hemoglobin/oxygen saturation
  - Tissue perfusion
```

### 1.3 Typical Characteristics

| Parameter | Value | Notes |
|-----------|-------|-------|
| Sampling Rate | 100-120 Hz | Project expects 100+ Hz |
| Frequency Range | 0.5-4 Hz | Normal resting heart rate band |
| Cardiac Cycle | 0.5-1.5s | 40-120 BPM |
| Signal Amplitude | Sensor-dependent | Raw ADC counts or normalized units |
| Noise Level | -30 to -20 dB | Motion artifact, ambient light, quantization |

### 1.4 Signal Quality Issues

Common problems in real-world PPG:
- **Motion Artifact**: Limb movement causes large amplitude noise
- **Ambient Light**: Fluorescent/LED lights (50/60 Hz interference)
- **Poor Contact**: Loose sensor contact → weak signal
- **Saturation**: Signal clipping at ADC limits
- **Baseline Drift**: Low-frequency trend from respiration, temperature changes

---

## 2. Signal Preprocessing

### 2.1 Resampling (Normalization to 120 Hz)

All models expect **exactly 120 Hz** sampling rate. Input data at different rates is resampled:

```python
src_rate = actual_rate_hz  # e.g., 100 Hz
n_src = len(ppg_segment)
duration = n_src / src_rate

# Target 120 Hz (MODEL_SAMPLING_RATE_HZ)
n_target = int(round(duration * 120))
ppg_resampled = resample(ppg_segment, n_target)
```

**Method**: `scipy.signal.resample` uses FFT-based interpolation
**Why**: Ensures consistent feature extraction across different data sources

**Example**:
```
30s @ 100Hz:  3000 samples → 3600 samples @ 120Hz
10s @ 50Hz:   500 samples  → 1200 samples @ 120Hz
60s @ 120Hz:  7200 samples → stays 7200 samples @ 120Hz
```

### 2.2 Median Filtering

Applied optionally for outlier suppression:

```python
ppg_med = scipy.signal.medfilt(ppg_segment, kernel_size=3)
```

**Purpose**:
- Removes impulse noise (single-sample spikes)
- Preserves signal edges better than low-pass filter
- Kernel size = 3 samples (≈25ms @ 120Hz)
- Minimal phase distortion

### 2.3 Noise Quality Gate

Before processing, signal is checked for viability:

```python
def _is_noisy(sig):
    return np.std(sig) < 1e-4 or np.max(sig) - np.min(sig) < 0.01
```

**Rejects signals where**:
- Standard deviation < 1e-4 (flat/constant signal)
- Peak-to-peak range < 0.01 (minimal amplitude)

**Examples**:
```
Good:    [620000, 620150, 620300, ...] → std=285, range=300 ✓
Bad:     [620000, 620001, 620000, ...] → std=0.5, range=1 ✗
```

---

## 3. Filtering

### 3.1 Bandpass Filter (Butterworth)

The most critical processing step:

```python
def _bandpass(sig):
    nyq = 0.5 * 120  # 60 Hz (Nyquist for 120Hz sampling)
    b, a = butter(3, [0.4 / nyq, 11.0 / nyq], btype="band")
    return filtfilt(b, a, sig)
```

**Specifications**:
- **Type**: Butterworth (maximally flat passband)
- **Order**: 3 (steep roll-off without ringing)
- **Method**: `filtfilt` (zero-phase, applies filter forward + backward)
- **Passband**: 0.4 - 11 Hz

**Frequency Response**:
```
Magnitude (dB)
0   |                    ╱╲
    |                 ╱╱    ╲╲
-3dB|         ___╱╱          ╲╲___
    |      ╱╱                      ╲╲
-20 |   ╱╱                            ╲╲
    |__╱                                ╲__
    0   0.4  1   2   3   4   5   8  10  11  20
                    Frequency (Hz)
```

**Purpose**:
1. **High-pass @ 0.4 Hz**: Removes DC offset and respiration (0.2-0.5 Hz)
2. **Low-pass @ 11 Hz**: Removes high-frequency noise and aliasing artifacts
3. **Zero-phase**: Preserves signal timing (critical for peak detection)

**Why These Frequencies?**
- 0.4 Hz = ~24 BPM (lower healthy resting limit)
- 11 Hz = ~660 BPM (upper physiological limit + safety margin)
- Removes motion artifact (~0.1-0.3 Hz) and 50/60 Hz mains noise

### 3.2 Signal Normalization

After filtering, signal normalized to [0,1] range:

```python
ppg_filt = bandpass(ppg_segment)
norm = (ppg_filt - ppg_filt.min()) / (ppg_filt.max() - ppg_filt.min() + 1e-6)
```

**Purpose**:
- Makes features independent of sensor type (ADC range varies)
- Standardizes feature values for consistent model predictions
- Prevents scale-dependent features from dominating

**Example**:
```
Raw ADC: [600000, 620000, 610000] → filtered → [0.0, 1.0, 0.5]
```

---

## 4. Cycle Detection

### 4.1 Peak and Trough Detection

Identifies systolic peaks and diastolic troughs:

```python
# Systolic peaks (local maxima)
peaks, _ = find_peaks(norm, distance=int(120 * 0.4))  # ≥0.4s apart

# Diastolic troughs (local minima)
mins, _ = find_peaks(-norm, distance=int(120 * 0.3))  # ≥0.3s apart
```

**Parameters**:
- **distance**: Minimum spacing between consecutive peaks
  - Peaks: 0.4s = minimum 150 BPM (faster than typical)
  - Troughs: 0.3s = physiological minimum spacing
- **height, prominence**: Optional thresholds (not used here)

**Algorithm**: Scipy's `find_peaks` uses iterative local maximum detection:
1. Find all local maxima
2. Filter by minimum distance
3. Return indices in original signal

**Output**:
```python
peaks = [120, 480, 840, ...]  # Sample indices of systolic peaks
mins = [0, 360, 720, ...]      # Sample indices of diastolic troughs
```

### 4.2 Cardiac Cycle Extraction

Segments signal into complete heartbeats:

```python
cycles = [norm[mins[mins < p][-1] : mins[mins > p][0]]
          for p in peaks
          if len(mins[mins < p]) > 0 and len(mins[mins > p]) > 0]
```

**Logic**:
- For each peak `p`:
  - Find last trough before peak: `mins[mins < p][-1]`
  - Find first trough after peak: `mins[mins > p][0]`
  - Extract segment between them
- Skip peaks with no surrounding troughs

**Visual**:
```
Signal:    [_________/\_________/\_________]
Troughs:           ^T1^         ^T2^
Peaks:                ^P1^           ^P2^
Cycle 1:           [T1...P1...T2] ← extracted
Cycle 2:                       [T2...P2...T3] ← extracted
```

**Validation**:
- Returns `None` if no cycles found
- Uses **largest cycle** (by peak value) for feature extraction
  - Reason: Most complete morphology, least artifact

---

## 5. Time-Domain Feature Extraction

### 5.1 Derivatives (Slopes and Acceleration)

```python
d1 = np.gradient(cycle)  # First derivative (velocity)
d2 = np.gradient(d1)     # Second derivative (acceleration)
```

**First Derivative `d1`**:
- Represents **slope** at each point
- Max value → steepest upstroke (systolic rise)
- Min value → steepest downstroke (diastolic fall)
- Features: max(d1), min(d1)

**Second Derivative `d2`** (Acceleration):
- Represents **curvature** changes
- Max value → peak of upstroke
- Min value → peak of downstroke
- Features: max(d2), min(d2)

**APG Waves** (from accelerated PPG):
```
d2 signal morphology:
        A wave (a) ← upstroke peak (systolic)
       /╲
      /  ╲_____ b (diastolic notch)
    /         ╲
  /            ╲_____ c,d,e waves (diastolic phase)

Features:
- a = max(d2)  ← systolic acceleration
- b = min(d2)  ← diastolic deceleration
- b/a ratio    ← vascular stiffness marker
```

### 5.2 Morphological Features

**Time to Peak (TTP)**:
```python
ttp = time_axis[np.argmax(cycle)]  # seconds from cycle start
```
- Measures systolic rise time
- Short TTP → steep vascular compliance
- Long TTP → reduced arterial elasticity

**Cycle Duration**:
```python
t_end = time_axis[-1]  # last point in cycle
```
- Total heartbeat period
- Related to heart rate: HR = 60 / duration

**TTP/TDP Ratio**:
```python
ratio = ttp / (t_end - ttp)
```
- Systolic vs diastolic time balance
- Indicates vascular properties

**Area Under Curve (AUC)**:
```python
auc = np.trapezoid(cycle, time_axis)
```
- Integral of PPG waveform
- Correlates with cardiac output and perfusion

### 5.3 Vascular Indices

**Reflection Index (RI)**:
```python
post_peak = cycle[peak_index:]
notch_mins, _ = find_peaks(-post_peak)
ri = post_peak[notch_mins[0]] / max(cycle)
```
- Ratio of diastolic notch to systolic peak
- Lower RI → better vascular elasticity

**Augmentation Index (AIx)**:
```python
aix = (notch_height - peak_height) / peak_height
```
- Positive: diastolic augmentation (stiffer vessels)
- Negative: diastolic amplification (compliant vessels)

**Large Artery Stiffness Index**:
```python
large_si = 0.1 / (ttp + 1e-9)
```
- Inverse relationship with time-to-peak
- Higher value → stiffer arteries

### 5.4 Heart Rate Variability (HRV)

**Inter-Beat Interval (IBI)**:
```python
ibi = np.diff(peaks) / 120  # seconds
```
- Time between consecutive heartbeats

**HRV (Standard Deviation)**:
```python
hrv = np.std(ibi)
```
- Variability in heart rate
- Low HRV (<5 bpm) → stress/sympathetic dominance
- High HRV (>15 bpm) → relaxation/parasympathetic dominance

**RMSSD** (Root Mean Square of Successive Differences):
```python
rmssd = np.sqrt(np.mean(np.diff(ibi)**2))
```
- High-frequency HRV component
- More sensitive to parasympathetic activity

**pNN50** (Percentage of NN intervals >50ms different):
```python
pnn50 = np.sum(np.abs(np.diff(ibi)) > 0.05) / len(ibi)
```
- Proportion of large interval changes
- High pNN50 → good vagal tone

### 5.5 Signal Statistics

```python
mean = np.mean(norm)
std  = np.std(norm)
max_val = np.max(norm)
min_val = np.min(norm)
```

- **Mean**: Average signal level (0 = baseline, 1 = peak)
- **Std**: Signal variability/contrast
- **Max/Min**: Dynamic range

---

## 6. Frequency-Domain Features

### 6.1 Fast Fourier Transform (FFT)

```python
fft_vals = np.abs(fft(cycle)[: len(cycle) // 2])
freqs = np.fft.fftfreq(len(cycle), 1 / 120)[: len(cycle) // 2]
```

**Steps**:
1. Apply FFT to cardiac cycle
2. Take absolute value (magnitude spectrum)
3. Extract only positive frequencies (Nyquist)
4. Compute corresponding frequency bins

**Why FFT?**
- Decompose signal into constituent frequencies
- Identify oscillatory components
- Extract power at different frequency bands

### 6.2 Dominant Frequency Extraction

```python
# Find spectral peaks
pks, _ = find_peaks(fft_vals, distance=5)

# Get top 3 by magnitude
top_idx = np.argsort(fft_vals[pks])[-3:]
top_freqs = list(freqs[pks][top_idx])
top_mags = list(fft_vals[pks][top_idx])
```

**Output Example**:
```
Frequency 1: 1.2 Hz (fundamental, heart rate)
Frequency 2: 2.4 Hz (2nd harmonic, overtone)
Frequency 3: 3.6 Hz (3rd harmonic, overtone)

Magnitude 1: 8500 (dominant component)
Magnitude 2: 2100 (second strongest)
Magnitude 3: 800 (weaker harmonic)
```

### 6.3 Spectral Features

| Feature | Purpose |
|---------|---------|
| Top frequency | Primary oscillation rate (heart rate) |
| Top magnitude | Power of dominant component |
| Harmonic pattern | Waveform regularity |
| Spectral entropy | Signal complexity |

---

## 7. Optical Quality Metrics

### 7.1 AC/DC Ratio

```python
ac_dc_ratio = (np.max(ppg_filt) - np.min(ppg_filt)) / (np.mean(np.abs(ppg_filt)) + 1e-9)
```

- **AC**: Peak-to-peak amplitude of pulsatile component
- **DC**: Average (static) signal level
- **Ratio**: Perfusion indicator
  - Higher ratio → better signal quality
  - Low ratio → poor contact or circulation

### 7.2 Signal Quality Index (SQI)

```python
sqi = np.max(cycle) / (np.std(ppg_filt) + 1e-6)
```

- Peak amplitude vs noise level
- Higher SQI → cleaner signal
- Threshold: SQI > 1.0 indicates acceptable quality

### 7.3 Perfusion Index

```python
perfusion_index = (np.max(ppg_filt) - np.min(ppg_filt)) / (np.mean(ppg_filt) + 1e-9)
```

- Relative amplitude of pulsatile component
- Correlates with peripheral perfusion
- Values: typically 0.02-0.50
- Low values → hypoperfusion risk

### 7.4 Signal Entropy

```python
sig_norm_e = ppg_filt - np.min(ppg_filt)
sig_norm_e = sig_norm_e / (np.sum(sig_norm_e) + 1e-9)  # Normalize to PDF
entropy = -np.sum(sig_norm_e * np.log(sig_norm_e + 1e-9))
```

- Shannon entropy of signal amplitude distribution
- Low entropy → regular, predictable waveform
- High entropy → irregular, noisy signal
- Used as feature for ML models

### 7.5 Skewness and Kurtosis

```python
from scipy.stats import skew, kurtosis
sig_skew = skew(ppg_filt)
cycle_skew = skew(cycle)
cycle_kurt = kurtosis(cycle)
```

- **Skewness**: Asymmetry of distribution
  - Positive: long right tail
  - Negative: long left tail
- **Kurtosis**: Peakedness vs flatness
  - High: sharp peaks (impulsive signals)
  - Low: flat distribution

---

## 8. Outlier Filtering

### 8.1 IQR (Interquartile Range) Method

```python
def _iqr_filter(values, factor=1.5):
    q1, q3 = np.percentile(values, 25), np.percentile(values, 75)
    iq = q3 - q1
    return (np.array(values) >= q1 - factor * iq) & (np.array(values) <= q3 + factor * iq)
```

**Algorithm**:
1. Calculate Q1 (25th percentile) and Q3 (75th percentile)
2. Compute IQR = Q3 - Q1
3. Define bounds:
   - Lower = Q1 - 1.5×IQR
   - Upper = Q3 + 1.5×IQR
4. Flag values outside bounds as outliers

**Example** (SBP predictions):
```
Values: [115, 118, 120, 125, 190]
Q1 = 117, Q3 = 127, IQR = 10
Lower = 117 - 15 = 102 ✓
Upper = 127 + 15 = 142 ✗
Outlier: 190 (outside [102, 142])
Result: [115, 118, 120, 125] (keep 4/5)
```

**Fallback Logic**:
- If >30% outliers detected, keep all values
- Rationale: Filter may be too aggressive on small samples

### 8.2 Segment-Level Filtering (BP Predictions)

```python
sbp_mask = _iqr_filter(sbp_predictions)
dbp_mask = _iqr_filter(dbp_predictions)
combined_mask = sbp_mask & dbp_mask
```

- Separate filtering for SBP and DBP
- Combined mask requires both to be inliers
- Uses ~6 segments, need ≥3 valid for final averaging

---

## 9. Pulse Width Metrics

### 9.1 Pulse Width at 50% Amplitude

```python
above_half = np.where(cycle >= np.max(cycle) * 0.5)[0]
pw50 = len(above_half) / 120  # seconds
```

- Duration signal stays above 50% of peak
- Correlates with vessel compliance
- Shorter width → stiffer vessels
- Longer width → more compliant vessels

### 9.2 Vascular Compliance Markers

| Metric | High Value | Low Value |
|--------|-----------|-----------|
| Pulse Width | Compliant vessels | Stiff vessels |
| AIx | Diastolic boost | Normal arteries |
| RI | Moderate reflection | Strong reflection |
| dP/dt (slope) | Rapid pressure rise | Gradual rise |

---

## 10. Data Validation Checklist

Before using extracted features for ML prediction:

| Check | Condition | Action |
|-------|-----------|--------|
| Signal length | < 30s @ 120Hz (3600 samples) | Skip prediction |
| Noise gate | std < 1e-4 or range < 0.01 | Skip prediction |
| Peak count | < 2 systolic peaks | Skip segment |
| Cycle count | < 1 complete cycle | Skip segment |
| Feature count | All 31/27 features valid | Check for NaN/Inf |
| Demographic | Age/BMI out of range | Apply defaults |
| Outliers | SBP >3 IQR | Remove segment |

---

## 11. Processing Pipeline Summary

```
Raw PPG (variable Hz)
    ↓
[Resample to 120 Hz] ← Normalize sampling rate
    ↓
[Noise check] ← Reject flat/clipped signals
    ↓
[Bandpass filter 0.4-11 Hz] ← Remove DC, respiration, noise
    ↓
[Normalize to [0,1]] ← Scale independence
    ↓
[Peak & trough detection] ← Identify heartbeats
    ↓
[Extract largest cycle] ← Best morphology
    ↓
[Time-domain features] ← Slopes, AUC, HRV
    ├─ Derivatives (d1, d2)
    ├─ Temporal metrics (TTP, tdp, ratio)
    ├─ Vascular indices (RI, AIx, SI)
    ├─ HRV metrics (std, RMSSD, pNN50)
    └─ Signal stats (mean, std, max, min)
    ↓
[Frequency-domain features] ← FFT analysis
    ├─ Top 3 frequencies
    ├─ Top 3 magnitudes
    └─ Spectral characteristics
    ↓
[Optical quality features] ← Signal quality
    ├─ AC/DC ratio
    ├─ Signal entropy
    ├─ Perfusion index
    ├─ SQI
    └─ Skewness/Kurtosis
    ↓
[Feature vector] ← 31 features for BP, 27 for Hb/Glu
    ↓
[Scale features] ← Apply StandardScaler
    ↓
[ML prediction] ← Model inference
    ↓
[Outlier filtering] ← IQR-based removal
    ↓
[Apply calibration] ← Personal offsets
    ↓
[Final result] ← SBP, DBP, Hb, Glucose
```

---

## 12. Troubleshooting Signal Issues

| Symptom | Likely Cause | Diagnosis | Fix |
|---------|--------------|-----------|-----|
| All NaN predictions | Noisy/flat signal | Check std dev and range | Improve sensor contact |
| Inconsistent cycle detection | Motion artifact | Inspect raw signal for spikes | Stabilize limb, filter more |
| Wrong peak spacing | Irregular rhythm | Inspect peak distances | Wait for steady state |
| High-frequency noise visible | Mains interference | FFT should show 50/60 Hz spike | Check power supply isolation |
| Baseline drift | Respiration | Slow oscillation below 0.4 Hz | Increase duration, filter captures it |
| Multiple peaks per beat | Bifid pulse | Normal in some populations | Process each peak separately |
| Loss of diastolic detail | Over-filtering | Examine filtered vs raw signal | Reduce filter order or edge freq |

---

## 13. Reference: Scipy Signal Functions

```python
# Filtering
from scipy.signal import butter, filtfilt, medfilt

# Peak detection
from scipy.signal import find_peaks

# Interpolation/Resampling
from scipy.signal import resample

# Fourier analysis
from scipy.fft import fft, fftfreq

# Statistical tests
from scipy.stats import iqr, skew, kurtosis
```

---

## 14. Key Constants (from config.py)

```python
MODEL_SAMPLING_RATE_HZ = 120     # Standard model rate
SAMPLING_RATE_HZ = 100           # Typical input rate
BANDPASS_LOW = 0.4               # Hz (respiration cutoff)
BANDPASS_HIGH = 11.0             # Hz (artifact cutoff)
BANDPASS_ORDER = 3               # Filter steepness
PEAK_DISTANCE = 0.4              # seconds (min inter-beat interval)
TROUGH_DISTANCE = 0.3            # seconds
IQR_FACTOR = 1.5                 # Multiplier for outlier detection
```

