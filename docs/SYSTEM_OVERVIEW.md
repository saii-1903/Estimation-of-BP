# LifeSigns — System Overview
**Version:** 6.0  
**Date:** 2026-05-11  
**Audience:** Product, clinical, and non-engineering stakeholders  
**Scope:** What the system does, who the actors are, how data flows — no code, no signal processing

---

## 1. What the System Does

LifeSigns is a continuous, non-invasive vitals monitoring service. Patients wear a PPG watch on their finger or wrist. Every 3 minutes the watch silently captures 30 seconds of pulse waveform data and sends it to the LifeSigns server. The server analyses the signal using an AI model and produces blood pressure (BP), haemoglobin (Hb), and blood glucose estimates for each packet. Results accumulate over a 15-minute session and a verified averaged reading is written to the database and surfaced on the clinical dashboard.

Each device and patient is **completely independent** — different patients can wear different device models simultaneously with no cross-patient dependency.

---

## 2. Actors and Devices

### The Patient
Wears one PPG watch continuously. The watch records pulse waveform data passively — the patient does not need to press a button or stay still (minor movement is tolerated).

### The Clinical Staff
Monitors BP readings and alerts on the dashboard. Responds when an alert fires, including taking a manual cuff reading when requested by the system.

### Supported Device Models

All devices send their data on **one Kafka topic** (`lifesigns.ppg.raw`). There is no separate Reference BP topic.

| Hardware / Protocol | Detected as | Native sampling rate | Built-in BP in payload? | Signal field |
|--------------------|------------|---------------------|------------------------|-------------|
| NISO204 | NISO204 | 120 Hz | Yes — `BPSystolic` / `BPDiastolic` (top-level); `404/200` = not measured this cycle | `Pleth` |
| CHECKME O2 / NISO103 | CHECKME | 120 Hz | Yes — `bp.bpSystolic` / `bp.bpDiastolic` (**ignored**) | `pleth.plethWave` |
| BerryMed Watch / NISO101 | BERRYMED | 100 Hz | Yes — `bp.bpSystolic` / `bp.bpDiastolic` (**ignored**) | `pleth.plethWave` |
| LS06 (reference cuff) | LS06 | — (no inference) | Yes — `bp.bpSystolic` / `bp.bpDiastolic` (**only this field used**) | `pleth.plethWave` (placeholder, ignored) |

**All wearable devices use AI (pleth) for BP estimation.** NISO204, NISO103, and NISO101 all run the AI model on the pleth signal. NISO204's built-in BP additionally serves as a per-packet cross-check: if it reads `404/200` (sentinel for "not measured"), the check is skipped. If the NISO204 device BP is valid and differs from the AI estimate by more than ±10 mmHg, clinical staff are alerted to take a manual reading.

**NISO103 and NISO101** built-in BP values are always ignored — only the pleth signal is used.

**LS06** is a dedicated reference BP cuff. Its `bp.bpSystolic / bp.bpDiastolic` is used to calibrate AI output. The plethWave field in LS06 messages is placeholder data and is **never** used for inference.

### The LifeSigns Server
Consumes PPG data, runs AI inference, performs mismatch detection, manages the reference validation state machine, aggregates session results, and writes to MongoDB.

### The Dashboard
Displays real-time BP readings per patient, active alerts, and session results.

---

## 3. End-to-End Data Flow

### 3.1 ASCII Flowchart — One Packet (every 3 minutes)

```
  Watch (NISO204 / NISO103 / NISO101 / LS06)
         |
         | Bluetooth
         v
  BLE Gateway  [parses raw BLE packets into JSON]
         |
         | Kafka Topic: lifesigns.ppg.raw
         |   (all device types on one topic)
         v
  ┌─────────────────────────────────────────────────────┐
  │              LifeSigns Server                       │
  │                                                     │
  │  1. DEVICE DETECTION                                │
  │     NISO204:  DeviceName == "NISO204"               │
  │     NISO103:  deviceType == "NISO103" → CHECKME     │
  │     NISO101:  deviceType == "NISO101" → BERRYMED    │
  │     LS06:     deviceType == "LS06"                  │
  │                    |                                │
  │     ┌─────── LS06? ────────────────────────────┐   │
  │     │  Extract bp.bpSystolic / bp.bpDiastolic  │   │
  │     │  Run state machine (see Section 6)       │   │
  │     │  Write reference_bp doc → MongoDB        │   │
  │     │  ← stop here for LS06                    │   │
  │     └──────────────────────────────────────────┘   │
  │                    |  (NISO204 / NISO103 / NISO101) │
  │  2. SIGNAL VALIDATION                               │
  │     Need ≥ 25s of data (≥ 3000 samples @ 120 Hz)   │
  │     If too short → ERROR doc → MongoDB              │
  │                    |                                │
  │  3. SIGNAL CLEANING (device-specific)               │
  │     NISO103: interpolate 156 sentinel values        │
  │     NISO101: strip leading zeros, remove spikes     │
  │     NISO204: remove spike artifacts                 │
  │                    |                                │
  │  4. MOTION GATE (per 5-second segment)              │
  │     Accelerometer data checked for each segment     │
  │     Excessive motion → segment discarded            │
  │     All 6 segments bad → SKIPPED (no result)        │
  │                    |                                │
  │  5. AI INFERENCE                                    │
  │     30 seconds of clean signal @ 120 Hz             │
  │     → raw SBP, DBP, BP category, Hb, Glucose        │
  │     If < 3 of 6 segments clean → ERROR              │
  │                    |                                │
  │  6. NISO204 MISMATCH CHECK  (NISO204 only)          │
  │     Compare device BPSystolic vs AI raw SBP         │
  │     If |diff| > 10 mmHg → bp_mismatch alert         │
  │     If device BP = 404/200 → skip check             │
  │                    |                                │
  │  7. REFERENCE CALIBRATION  (all pleth devices)      │
  │     Look up patient state (no_reference /           │
  │     unconfirmed / normal / breach_pending /         │
  │     case2_pending).                                 │
  │     If normal/breach_pending/case2_pending:         │
  │         output = AI_raw + offset                    │
  │     If no_reference or unconfirmed:                 │
  │         output = raw AI values                      │
  │     Track latest AI reading (for next LS06 check)   │
  │     Drift check: session-level only (see step 9)    │
  │                    |                                │
  │  8. SESSION AGGREGATOR                              │
  │     Each valid result → added to session buffer     │
  │     3-min deduplication: latest replaces earlier    │
  │     Session completes when 5 readings collected     │
  │     (or 15 minutes elapsed)                         │
  │                    |                                │
  │  9. SESSION RESULT                                  │
  │     Outlier removal (>10 mmHg from median removed)  │
  │     Need ≥ 3 good readings (of 5) to publish        │
  │     Average SBP/DBP/Hb/Glucose of good readings     │
  │                    |                                │
  │  10. TREND CHECK  (all modes)                       │
  │     Compare session avg with previous session       │
  │     If |diff| > 15 mmHg (SBP or DBP) → bp_trend    │
  │                    |                                │
  └─────────────────────────────────────────────────────┘
         |
         | MongoDB write (one per 15-min session)
         v
  vitals_db.ppg_vitals_results
         |
         v
  Dashboard displays reading + any alert
```

---

## 4. Signal Processing — How Device Differences Are Handled

| Step | What it does | Applies to |
|------|-------------|-----------|
| Sentinel removal | Interpolates the value `156` (finger-off marker) | NISO103 only |
| Leading-zero removal | Strips ADC startup zeros | NISO101 only |
| Spike removal | Median-filter-based outlier replacement | All devices |
| Resampling | 100 Hz → 120 Hz so model sees uniform rate | NISO101 only |
| Baseline drift removal | High-pass filter removes slow DC drift | All |
| Smoothing | Preserves peak shape, removes electrical noise | All |
| Amplitude normalisation | Removes ADC scale differences across devices | All |
| Motion gate | Per-segment accelerometer check | NISO101, NISO103, LS06 |
| 7-gate noise check | Flat signal, clipping, skewness, kurtosis, etc. | All |

Minimum requirement: **3 of 6 segments clean** (each segment = 5 seconds). If fewer clean segments remain, the packet is discarded as an error.

---

## 5. Session Aggregation — Why 5 Readings?

A single 30-second PPG capture can have measurement variance. LifeSigns uses a 15-minute session of 5 readings to produce a stable output:

```
  Packet 1  (t = 0 min)   → reading stored
  Packet 2  (t = 3 min)   → reading stored
  Packet 3  (t = 6 min)   → reading stored
  Packet 4  (t = 9 min)   → reading stored
  Packet 5  (t = 12 min)  → reading stored → session complete

  Outlier removal:
    Any reading > 10 mmHg from the session median is removed.
    At least 3 of 5 must survive → averaged and published.

  One MongoDB write per 15-minute session.
```

**Deduplication rule:** If two packets arrive within 3 minutes of each other (e.g. a manual device resend), only the latest one is kept in the session buffer.

**Session gap reset:** If the watch is silent for more than 4 minutes (watch removed, BLE dropped), the session buffer auto-resets on the next packet so the new session starts clean.

---

## 6. Reference Validation State Machine (5 States)

The system validates the LS06 reference reading against the AI estimate before applying calibration, and provides a structured recovery path if BP drifts after calibration.

**Key design principles:**
- The **offset** (AI correction factor) is computed once at first confirmation and **never changed again**.
- Only the **baseline reference** is updated when a new LS06 arrives in `normal` state.
- **Drift is detected at session level** (15-min averages), not on individual packets.

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  State: no_reference                                                │
  │  • No LS06 seen for this patient                                    │
  │  • AI output shown as-is (raw, no offset)                          │
  │  • Trend alert fires if session change > ±15 mmHg                  │
  └─────────────────────────────────────────────────────────────────────┘
       |                               |
  LS06 arrives,               LS06 arrives,
  |LS06 − AI| ≤ 10 mmHg      |LS06 − AI| > 10 mmHg
  (agrees or no AI yet)       (disagrees)
       |                               |
       v                               v
  ┌───────────────────────┐  ┌──────────────────────────────────────────┐
  │  State: normal         │  │  State: unconfirmed                      │
  │  (zero offset)         │  │  • Still showing raw AI                  │
  └───────────────────────┘  │  • Alert: "take another manual reading"  │
                              │  • Waiting for second LS06               │
                              └──────────────────────────────────────────┘
                                            |
                                Second LS06 always force-confirms
                                            |
                        ┌───────────────────┴───────────────────┐
                        |                                        |
              |LS06 − AI| ≤ 5 mmHg                   |LS06 − AI| > 5 mmHg
              (AI was right)                          (reference wins)
                        |                                        |
                        v                                        v
                normal, zero offset            normal, offset = LS06 − AI_raw
                                               + immediate calibration snapshot

  ┌─────────────────────────────────────────────────────────────────────┐
  │  State: normal                                                      │
  │  • output = AI_raw + offset                                        │
  │  • New LS06 → update baseline ref only, offset UNCHANGED           │
  │  • Trend alert fires if session change > ±15 mmHg                  │
  │  • Session drift check: if |session_avg − baseline| > ±10 mmHg     │
  │      → breach_pending + bp_drift alert                              │
  └─────────────────────────────────────────────────────────────────────┘
       |
  Session drift > ±10 mmHg
       |
       v
  ┌─────────────────────────────────────────────────────────────────────┐
  │  State: breach_pending                                              │
  │  • bp_drift alert fired, clinical staff notified                   │
  │  • Calibration continues with existing offset                      │
  │  • Sessions still written; drift check skipped (no double-alert)   │
  │  • Waiting for new LS06 to set pending reference                   │
  │  • After LS06 arrives: collect 5 individual AI readings vs cuff    │
  │      ≥ 3 match (|diff| ≤ 10 mmHg) → breach_resolved → normal      │
  │      < 3 match → escalate to case2_pending                        │
  │  • New LS06 always replaces pending_ref, resets reading counter    │
  └─────────────────────────────────────────────────────────────────────┘
       |                               |
  ≥ 3 of 5 match               < 3 of 5 match
       |                               |
       v                               v
  ┌──────────────────────┐  ┌──────────────────────────────────────────┐
  │ breach_resolved       │  │  State: case2_pending                    │
  │ → normal              │  │  • bp_drift_escalation alert fired       │
  └──────────────────────┘  │  • Session aggregator reset              │
                              │  • Waiting for one full 15-min session   │
                              │  • New LS06: update pending_ref only     │
                              │  • Verification session completes:       │
                              │      pending_ref is ground truth either  │
                              │      way → baseline = pending_ref        │
                              │      → breach_resolved → normal          │
                              └──────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────────────┐
  │  State: normal (after breach_resolved)                              │
  │  • baseline updated to pending_ref                                  │
  │  • offset unchanged — AI correction is personal, BP change is not  │
  │  • bp_alert = "breach_resolved" in resolution session doc          │
  │  • New drift on future sessions restarts the breach cycle          │
  └─────────────────────────────────────────────────────────────────────┘
```

**Why the offset never changes after breach resolution:**  
The offset corrects for a systematic personal difference between this patient's AI-predicted BP and actual cuff BP. That difference is a property of the patient's physiology, not their current BP level. When BP drifts up or down, only the baseline reference needs updating — the AI model's per-patient error stays the same.

---

## 7. Alert Types

| Alert type | Trigger | Field in doc | State |
|-----------|---------|-------------|-------|
| `bp_mismatch` | NISO204 device BP vs AI raw > ±10 mmHg (per packet) | `bp_alert` | Any |
| `reference_mismatch` | LS06 vs latest AI disagrees > ±10 mmHg | `bp_alert` on reference_bp doc | no_reference → unconfirmed |
| `bp_drift` | 15-min session avg drifts > ±10 mmHg from baseline | `bp_alert` on session doc | normal → breach_pending |
| `bp_drift_escalation` | 5 post-breach readings fail majority match (< 3 of 5) | `bp_alert` on session doc | breach_pending → case2_pending |
| `breach_resolved` | Breach recovery confirmed — baseline updated | `bp_alert` on resolution doc | breach_pending/case2_pending → normal |
| `bp_trend` | Session-to-session change > ±15 mmHg (SBP or DBP) | `bp_alert` (or `bp_trend_alert` if another alert exists) | All |

**Alert priority:** `bp_drift` / `bp_drift_escalation` / `breach_resolved` > `bp_mismatch` > `bp_trend`. If a trend alert fires alongside a drift-family alert, it is stored in `bp_trend_alert` so neither is lost.

**Drift detection is session-level.** Individual AI packets are never checked for drift — only completed 15-min session averages are compared against the baseline. This avoids false alerts from noisy single readings.

No alert blocks the session result from being written — alerts and results are stored together. Clinical staff decide the next action.

---

## 8. Hb and Glucose Reporting

Hb (haemoglobin) and glucose are estimated by the AI model from the same PPG signal alongside BP. They are included in every successful session result for all device types. There is no separate calibration requirement for these values — the model produces them directly from the signal.

If the AI model fails or the signal is insufficient, Hb and Glucose are null in the result (same error condition as BP).

---

## 9. Data Quality — What Happens to Bad Readings

| Situation | What happens |
|-----------|-------------|
| Patient moving during the 30s window | Affected segments discarded. If < 3 clean remain, packet rejected. |
| Signal too noisy (finger off, poor contact) | Segment fails noise gate, discarded. |
| NISO103 short finger-off gap (< 1s, value 156) | Interpolated — reading continues normally. |
| NISO103 long finger-off gap (> 1s) | Segment discarded by noise gate. |
| NISO101 ADC startup zeros | Stripped before processing. |
| Insufficient signal (< 25s of data) | Packet rejected immediately, logged as error. |
| Watch silent for > 4 minutes | Session buffer resets automatically on next packet. |
| Session with < 3 good readings after outlier removal | No result published; session error logged. |
| LS06 bpError=1 or missing BP block | Written as error doc; state machine not triggered. |

---

## 10. Output Summary — What Gets Written and When

| Output type | Trigger | Content |
|-------------|---------|---------|
| `session_complete` | Every 15 minutes (5 readings collected and averaged) | SBP, DBP, BP category, Hb, Glucose, bp_alert (if any), bp_trend_alert (if any) |
| `calibration_applied` | First LS06 confirms with non-zero offset | Immediate calibrated reading so dashboard doesn't wait 15 min for first corrected value |
| `breach_resolved` | Breach recovery confirmed (in session doc bp_alert) | Session result + new baseline, offset |
| `reference_bp` | LS06 message received | reference_bp (sbp/dbp), bp_alert if mismatch with AI |
| `error` | Insufficient signal, all segments noisy, inference failure | Reason, admissionId |
| `skipped` | All 6 segments have excessive motion | Reason (informational, no clinical action) |

**Hb and Glucose:** Reported in every `session_complete` doc. Suppressed (`null`) only when BP is absent — i.e., when the AI model fails to produce a BP estimate. If BP is present but any alert is active, Hb and Glucose are still reported normally.

---

## 11. Test Tool — berry_app.py

A separate local test server (`berry_app.py`) exists for development use only. It accepts PPG JSON via HTTP POST, runs inference immediately, and streams results live to a test dashboard via SSE. It supports all four device types (NISO204, NISO103, NISO101, LS06). Any calibration or state machine logic in berry_app.py is for test purposes only and does not reflect the production pipeline.

---

## 12. What the System Does NOT Do

- **Does not replace a clinical diagnosis.** Alerts prompt human review, not treatment decisions.
- **Does not handle Bluetooth directly.** BLE connection and packet parsing is handled by the gateway.
- **Does not use a separate Kafka topic for reference BP.** All devices send their data on one topic.
- **Does not use NISO103 / NISO101 built-in BP for any comparison.** Only NISO204 and LS06.
- **Does not apply calibration until the reference is confirmed.** Raw AI values are shown while the state is no_reference or unconfirmed.
- **Does not mix data across patients.** Each device/patient is processed completely independently.
