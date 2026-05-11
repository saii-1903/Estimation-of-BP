# LifeSigns — Processing Cases Reference
**Version:** 6.0  
**Date:** 2026-05-11  
**Scope:** All scenarios the signal processing, session aggregation, BP mismatch alert,
           reference validation state machine (5-state with breach recovery), and trend detection handle.

---

## Architecture

All devices send PPG pleth and any built-in BP measurement on **one Kafka topic** (`lifesigns.ppg.raw`).  
There is no separate Reference BP topic. No personal calibration offsets are stored until confirmed.

| Device / Protocol | Detected as | AI inference? | Built-in BP used? | Signal field |
|------------------|------------|--------------|------------------|-------------|
| NISO204 | NISO204 | Yes | Yes — mismatch alert; `404/200` = skip check | `Pleth` (top-level) |
| CHECKME O2 / NISO103 | CHECKME | Yes | **No — ignored** | `pleth.plethWave` |
| BerryMed Watch / NISO101 | BERRYMED | Yes | **No — ignored** | `pleth.plethWave` |
| LS06 (reference cuff) | LS06 | **No** | Yes — only field used | `pleth.plethWave` (placeholder, ignored) |

---

## Reference Validation State Machine (5 States)

Each patient (keyed by `admissionId`) moves through five states. The **offset** — the AI correction factor — is computed **once** on first confirmation and **never changed again**. Only the baseline reference (`ref_sbp`/`ref_dbp`) is updated on new LS06 arrivals in `normal` state.

**Drift detection is session-level only.** Individual packets are calibrated with the offset but never trigger a drift alert. Only a completed 15-min session average is compared against the baseline.

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  no_reference                                                    │
  │  • Raw AI output shown as-is (no offset applied)                │
  │  • Trend alert if session change > ±15 mmHg                     │
  │  • LS06 arrives → compare with latest AI:                       │
  │      |LS06 − AI| ≤ 10 → normal (zero offset)                    │
  │      |LS06 − AI| > 10 → unconfirmed + reference_mismatch alert  │
  │      No AI reading yet → unconfirmed (store reference, no alert) │
  └──────────────────────────────────────────────────────────────────┘
              |
    LS06 arrives, disagrees (|diff| > 10)
              |
              v
  ┌──────────────────────────────────────────────────────────────────┐
  │  unconfirmed                                                     │
  │  • Raw AI output shown (no calibration yet)                     │
  │  • Trend alert still fires                                       │
  │  • Second LS06 always force-confirms:                           │
  │      |LS06 − AI| ≤ 5 (LS06_CONFIRM_TOLERANCE)                   │
  │          → normal, offset = 0 (AI was correct)                  │
  │      |LS06 − AI| > 5                                            │
  │          → normal, offset = LS06 − AI_raw (offset frozen here)  │
  └──────────────────────────────────────────────────────────────────┘
              |
    Second LS06 arrives
              |
              v
  ┌──────────────────────────────────────────────────────────────────┐
  │  normal                                                          │
  │  • Calibration active: output = AI_raw + offset                 │
  │  • New LS06 in normal state:                                    │
  │      → update baseline (ref_sbp/dbp) only                       │
  │      → offset_sbp/dbp UNCHANGED — never recomputed              │
  │      → stay in normal, no alert, no re-validation               │
  │  • Session drift check: if |session_avg − baseline| > 10 mmHg   │
  │      → breach_pending + bp_drift alert                           │
  │  • Trend alert still fires in all sessions                      │
  └──────────────────────────────────────────────────────────────────┘
              |
    Session drift exceeds ±10 mmHg
              |
              v
  ┌──────────────────────────────────────────────────────────────────┐
  │  breach_pending                                                  │
  │  • bp_drift alert fired                                          │
  │  • Calibration continues with existing offset                   │
  │  • Waiting for a new LS06 reading (pending_ref)                 │
  │  • Sessions completing during this state: written, drift skipped │
  │  • When LS06 arrives:                                           │
  │      → pending_ref set, post_breach_readings reset              │
  │  • After LS06 arrives, collect up to 5 individual AI readings:  │
  │      ≥ 3 of 5 match pending_ref (|diff| ≤ 10)                  │
  │          → breach_resolved, baseline = pending_ref, → normal    │
  │      < 3 of 5 match                                             │
  │          → bp_drift_escalation alert, → case2_pending           │
  │  • No LS06 → state stays breach_pending indefinitely            │
  └──────────────────────────────────────────────────────────────────┘
              |
    < 3 of 5 post-breach readings match pending_ref
              |
              v
  ┌──────────────────────────────────────────────────────────────────┐
  │  case2_pending                                                   │
  │  • bp_drift_escalation alert fired                               │
  │  • Session aggregator was reset (fresh 15-min window)           │
  │  • New LS06 during this state: update pending_ref only          │
  │  • When the next full 15-min session completes:                 │
  │      → verification session: accept pending_ref as ground truth │
  │      → baseline = pending_ref, offset unchanged                 │
  │      → breach_resolved in session doc, → normal                 │
  └──────────────────────────────────────────────────────────────────┘
```

**State persistence:**
- `offset_sbp` / `offset_dbp`: set once at first confirmation, never modified again
- `ref_sbp` / `ref_dbp`: updated whenever a new LS06 arrives in `normal` state
- `pending_ref_sbp` / `pending_ref_dbp`: staged reference during breach; becomes the baseline on resolution
- `post_breach_readings`: individual AI readings collected after LS06 arrives in breach_pending

---

## Group 0 — Initial Reference Validation (no_reference → unconfirmed → normal)

### Case 0a: First LS06 agrees with AI → normal immediately

```
Latest AI reading: sbp=127, dbp=82
LS06 arrives: bp.bpSystolic=130, bp.bpDiastolic=85

|130 − 127| = 3 ≤ 10 → agrees
|85  − 82|  = 3 ≤ 10 → agrees

→ status: normal, offset: 0/0 (zero offset, AI was already accurate)
→ baseline: ref_sbp=130, ref_dbp=85
→ reference_bp doc written to MongoDB (no alert)
→ Next AI packet (raw: 127/82) → calibrated: 127+0 / 82+0 = 127/82 (unchanged)
```

### Case 0b: First LS06 disagrees with AI → unconfirmed, alert

```
Latest AI reading: sbp=127, dbp=82
LS06 arrives: bp.bpSystolic=150, bp.bpDiastolic=95

|150 − 127| = 23 > 10 → disagrees

→ status: unconfirmed
→ reference_bp doc written to MongoDB with bp_alert:
    {
      type: "reference_mismatch",
      message: "Reference BP and AI estimates disagree — take another manual reading",
      reference_bp: {sbp: 150, dbp: 95},
      ai_bp:         {sbp: 127, dbp: 82},
      diff_sbp: 23.0, diff_dbp: 13.0
    }
→ AI packets continue with raw values (no calibration, no offset)
```

### Case 0c: No AI reading when first LS06 arrives

```
No AI reading received yet for this patient
LS06 arrives: bp.bpSystolic=130, bp.bpDiastolic=85

→ status: unconfirmed (nothing to compare against)
→ reference_bp doc written (no alert — mismatch requires an AI reading to compare)
→ AI packets continue with raw values until second LS06
```

### Case 0d: Second LS06 ≈ AI (within ±5) → normal, zero offset

```
Status: unconfirmed
Latest AI reading: sbp=127, dbp=82
Second LS06: bp.bpSystolic=128, bp.bpDiastolic=83

|128 − 127| = 1 ≤ 5 → within LS06_CONFIRM_TOLERANCE
|83  − 82|  = 1 ≤ 5 → within LS06_CONFIRM_TOLERANCE

→ status: normal, offset: 0/0 (AI was right all along, no correction needed)
→ baseline: ref_sbp=128, ref_dbp=83
→ No alert
→ Next AI packet (raw: 127/82) → calibrated: 127/82 (offset is zero)
```

### Case 0e: Second LS06 force-confirms with non-zero offset → immediate snapshot

```
Status: unconfirmed
Latest AI reading: sbp=127, dbp=82
Second LS06: bp.bpSystolic=150, bp.bpDiastolic=95

|150 − 127| = 23 > 5 → outside LS06_CONFIRM_TOLERANCE

→ status: normal
→ offset_sbp = 150 − 127 = +23   ← FROZEN — never changes again
→ offset_dbp = 95  − 82  = +13   ← FROZEN — never changes again
→ baseline: ref_sbp=150, ref_dbp=95

Immediate calibration snapshot written to MongoDB (processingStatus: "calibration_applied"):
  → cal_sbp = 127 + 23 = 150
  → cal_dbp = 82  + 13 = 95
  (This gives dashboard an immediate corrected reading without waiting 15 min)

Next AI packet (raw: 130/84):
  → calibrated: 130+23 / 84+13 = 153/97
  → session proceeds with calibrated values
  → drift check happens at session-level only (not per-packet)
```

### Case 0f: Session average drifts from baseline → breach_pending

```
Status: normal, baseline: 150/95, offset: +23/+13

AI packets (raw): 143/91, 141/90, 145/92, 140/89, 142/91
  → calibrated: 166/104, 164/103, 168/105, 163/102, 165/104

Session average: sbp=165.2, dbp=103.6

|165.2 − 150| = 15.2 > 10 → DRIFT detected at session level

→ status: breach_pending
→ Session doc written with bp_alert:
    {
      type: "bp_drift",
      message: "BP has changed significantly from reference — take manual reading",
      reference_bp: {sbp: 150, dbp: 95},
      current_bp:   {sbp: 165, dbp: 104},
      diff_sbp: 15.2, diff_dbp: 8.6
    }
→ Waiting for new LS06 reading to begin breach resolution
```

### Case 0g: New LS06 in normal state → update baseline only (no re-validation)

```
Status: normal, baseline: 150/95, offset: +23/+13
Latest AI reading: sbp=130, dbp=84
New LS06: bp.bpSystolic=155, bp.bpDiastolic=98

→ status stays: normal (no re-validation, no re-confirmation cycle)
→ ref_sbp updated: 150 → 155
→ ref_dbp updated: 95  → 98
→ offset_sbp stays: +23 (unchanged)
→ offset_dbp stays: +13 (unchanged)
→ reference_bp doc written (no alert)
→ Next drift check will use new baseline 155/98
```

### Case 0h: AI packet with no reference ever received

```
No LS06 received, no state entry for this patient

AI packet (raw: 125/80):
  → latest_ai_sbp/dbp stored internally for future LS06 comparison
  → calibrated = raw (no offset, no calibration)
  → bp_alert = null
  → Session proceeds with raw values
```

---

## Group 0T — Trend Detection (all modes)

### Case 0T-1: First session — no previous data

```
Session 1 completes: sbp=122, dbp=79
→ Stored as last session for this patient
→ No trend alert (nothing to compare against)
```

### Case 0T-2: Second session within threshold — no alert

```
Session 1: sbp=122, dbp=79
Session 2: sbp=130, dbp=82

|130 − 122| = 8 ≤ 15 → OK
|82  − 79|  = 3 ≤ 15 → OK

→ No trend alert
→ Session 2 stored as new last session
```

### Case 0T-3: SBP trend exceeds threshold → trend alert

```
Session 1: sbp=122, dbp=79
Session 2: sbp=142, dbp=82

|142 − 122| = 20 > 15 → TREND

→ bp_alert = {
    type: "bp_trend",
    message: "BP has changed significantly since last session — review recommended",
    previous_bp: {sbp: 122, dbp: 79},
    current_bp:  {sbp: 142, dbp: 82},
    diff_sbp: 20.0, diff_dbp: 3.0
  }
→ Either SBP or DBP exceeding 15 is sufficient to trigger
```

### Case 0T-4: Trend alert coexists with drift alert

```
Status: normal — both bp_drift and bp_trend fire in the same session

→ bp_alert = drift alert (drift takes priority in the main field)
→ bp_trend_alert = trend alert (stored as separate field in the session doc)
```

---

## Group B — Breach Detection & Recovery (normal → breach_pending → case2_pending → normal)

These cases cover the full lifecycle after a drift breach is detected. The system collects evidence (individual AI readings vs. new LS06) before declaring the breach resolved or escalating.

### Case B1: Session drift fires → enter breach_pending

```
Status: normal, baseline: sbp=150, dbp=95, offset: +23/+13

Session 2 average: sbp=168, dbp=108
|168 − 150| = 18 > 10 → DRIFT

→ status: breach_pending
→ Session doc written with bp_alert.type = "bp_drift"
→ post_breach_readings = []   (waiting for new LS06 before counting)
→ pending_ref not yet set
```

### Case B2: LS06 arrives during breach_pending → pending_ref set

```
Status: breach_pending, no pending_ref yet

LS06 arrives: bp.bpSystolic=168, bp.bpDiastolic=108

→ pending_ref_sbp = 168, pending_ref_dbp = 108
→ post_breach_readings reset to []   (now start counting individual AI readings)
→ reference_bp doc written (no alert — this is the expected re-measurement)
→ status stays: breach_pending
```

### Case B3: 5 post-breach readings collected — ≥3 match → breach_resolved, back to normal

```
Status: breach_pending, pending_ref: 168/108, offset: +23/+13

Post-breach AI readings (raw) and calibrated results:
  Reading 1: raw 145/95 → calibrated 168/108 → |168−168|=0  ≤ 10 → match ✓
  Reading 2: raw 146/96 → calibrated 169/109 → |169−168|=1  ≤ 10 → match ✓
  Reading 3: raw 144/94 → calibrated 167/107 → |167−168|=1  ≤ 10 → match ✓
  Reading 4: raw 145/95 → calibrated 168/108 → |168−168|=0  ≤ 10 → match ✓
  Reading 5: raw 155/100 → calibrated 178/113 → |178−168|=10 ≤ 10 → match ✓

5 readings collected. 5 of 5 match → ≥ 3 threshold met

→ breach_resolved
→ baseline updated: ref_sbp = 168, ref_dbp = 108
→ offset unchanged: +23/+13
→ status: normal
→ MongoDB doc written with processingStatus="breach_resolved"
```

### Case B4: 5 post-breach readings collected — < 3 match → escalate to case2_pending

```
Status: breach_pending, pending_ref: 168/108, offset: +23/+13

Post-breach AI readings (calibrated):
  Reading 1: 168/108 → match ✓
  Reading 2: 152/96  → |152−168|=16 > 10 → no match ✗
  Reading 3: 150/95  → |150−168|=18 > 10 → no match ✗
  Reading 4: 151/94  → |151−168|=17 > 10 → no match ✗
  Reading 5: 149/93  → |149−168|=19 > 10 → no match ✗

5 readings collected. 1 of 5 match → < 3 threshold

→ bp_drift_escalation alert fired
→ status: case2_pending
→ Session aggregator reset (fresh 15-min window)
→ MongoDB doc written with bp_alert.type = "bp_drift_escalation"
```

### Case B5: Verification session matches pending_ref → breach_resolved, back to normal

```
Status: case2_pending, pending_ref: 168/108, offset: +23/+13

Verification session (fresh 15-min session, 5 readings collected):
  Session average: sbp=166, dbp=107

Verification result: pending_ref is accepted as ground truth
  → baseline updated: ref_sbp = 168, ref_dbp = 108
  → offset unchanged: +23/+13
  → status: normal
  → Session doc written with bp_alert.type = "breach_resolved"
```

### Case B6: Verification session does NOT match pending_ref — cuff overrides anyway

```
Status: case2_pending, pending_ref: 168/108, offset: +23/+13

Verification session average: sbp=150, dbp=95
|150 − 168| = 18 > 10 → session disagrees with pending_ref

The LS06 reference cuff is the clinical ground truth.
→ pending_ref wins regardless of session value
→ baseline updated: ref_sbp = 168, ref_dbp = 108
→ offset unchanged: +23/+13
→ status: normal
→ Session doc written with bp_alert.type = "breach_resolved"
  (and any trend alert in bp_trend_alert if applicable)
```

### Case B7: New LS06 arrives during breach_pending — replaces pending_ref, counter resets

```
Status: breach_pending
pending_ref: 168/108 (set from first LS06)
post_breach_readings: [168/108, 152/96, 150/95]  (3 readings collected so far)

New LS06 arrives: bp.bpSystolic=165, bp.bpDiastolic=106

→ pending_ref updated: 168/108 → 165/106  (always use the most recent cuff reading)
→ post_breach_readings reset to []   (restart the 5-reading window)
→ reference_bp doc written (no alert)
→ status stays: breach_pending
```

### Case B8: New LS06 arrives during case2_pending — update pending_ref only

```
Status: case2_pending
pending_ref: 168/108

New LS06 arrives: bp.bpSystolic=165, bp.bpDiastolic=106

→ pending_ref_sbp = 165, pending_ref_dbp = 106  (updated)
→ verification session aggregator NOT reset (already collecting a fresh session)
→ reference_bp doc written (no alert)
→ status stays: case2_pending
```

### Case B9: No LS06 arrives during breach_pending — state waits indefinitely

```
Status: breach_pending, pending_ref: None (no LS06 yet)

AI readings continue to arrive:
  → calibrated with existing offset
  → added to session buffer as normal
  → post_breach_readings NOT populated (pending_ref not set, no counting)
  → sessions written normally but drift check skipped (already in breach)

Clinical staff must take a manual LS06 reading to advance state.
```

### Case B10: Session completes during breach_pending — drift check skipped

```
Status: breach_pending, pending_ref: 168/108, post_breach_readings collecting

Next 15-min session completes (5 readings, average: sbp=169, dbp=108)

→ Session written to MongoDB as session_complete
→ Drift check SKIPPED — already in breach_pending; no double-alert
→ Trend check still runs as normal
→ post_breach_readings count toward individual-reading comparison (separate from session)
```

### Case B11: New LS06 in normal state — baseline updates, offset unchanged

```
Status: normal, baseline: sbp=150, dbp=95, offset: +23/+13
Latest AI reading: sbp=130, dbp=84

LS06 arrives: bp.bpSystolic=153, bp.bpDiastolic=97

→ ref_sbp updated: 150 → 153
→ ref_dbp updated: 95  → 97
→ offset_sbp stays: +23  (frozen — never changes)
→ offset_dbp stays: +13  (frozen — never changes)
→ status stays: normal
→ reference_bp doc written (no alert)

(This is routine monitoring data — patient's BP changed, AI model is still the same person,
 so the same AI correction factor still applies.)
```

### Case B12: Repeat breach after resolution — fresh breach cycle

```
Status: normal (just resolved from previous breach)
baseline: sbp=168, dbp=108, offset: +23/+13

Session 5 average: sbp=185, dbp=120
|185 − 168| = 17 > 10 → new DRIFT

→ status: breach_pending  (fresh cycle — pending_ref cleared, post_breach_readings=[])
→ bp_drift alert fired as before
→ Resolution follows Cases B2–B3 or B4–B5/B6 again
```

---

## Group 1 — Signal Validation

### Case 1: Signal too short
```
Packet arrives with plethWave of only 1500 samples (< 3000 minimum)
  → Immediate error result
  → Error doc written to MongoDB: {processingStatus: "error", reason: "Insufficient data"}
  → No session accumulation
```

### Case 2: All segments motion-contaminated
```
Accelerometer data shows excessive motion in all 6 segments
  → Packet skipped: {processingStatus: "skipped"}
  → No session accumulation, no alert
```

### Case 3: Too few clean segments after noise gate
```
Packet passes length check, but only 2 of 6 segments survive noise gate (need ≥ 3)
  → Error doc: "Only 2 of 6 segments were clean (10s). Need at least 15s."
  → No session accumulation
```

### Case 4: Partial motion — some segments clean
```
2 of 6 segments flagged for motion, 4 clean
  → 4 clean segments processed normally
  → Result produced and added to session buffer
```

---

## Group 2 — Session Aggregation

### Case 5: Normal session — 5 readings, clean data
```
Readings collected at t=0, 3, 6, 9, 12 min:
  SBP: [121, 119, 123, 118, 120]
  Session median SBP: 120
  All within ±10 mmHg of median → 5/5 good readings
  Average: SBP=120.2, DBP=79.0
  → session_complete written to MongoDB
```

### Case 6: Outlier in session — removed automatically
```
Readings:
  SBP: [120, 118, 119, 117, 145]
  Session median SBP: 119
  |145 − 119| = 26 > 10 → outlier removed
  → 4/5 good readings, average of 4: SBP=118.5
  → session_complete written (session_summary.outliers_removed: 1)
```

### Case 7: Too many outliers — session fails
```
Readings:
  SBP: [120, 155, 160, 158, 119]
  Session median SBP: 155
  |120 − 155| = 35 → outlier
  |119 − 155| = 36 → outlier
  → only 3 good readings (155, 160, 158)
  → ≥ 3 required → session publishes with 3 good readings

  If only 2 good readings survived:
  → session error: "Noisy data — only 2 of 5 readings were consistent."
  → no MongoDB write
```

### Case 8: 3-minute deduplication (rapid re-send)
```
1:03  Packet A → buffer: [A]
1:05  Packet B → gap 2 min < 3 min → REPLACE A → buffer: [B]
1:08  Packet C → gap 3 min ≥ 3 min → ADD → buffer: [B, C]
1:10  Packet D → gap 2 min < 3 min → REPLACE C → buffer: [B, D]
1:13  Packet E → gap 3 min ≥ 3 min → ADD → buffer: [B, D, E]
1:16  Packet F → ADD → buffer: [B, D, E, F]
1:19  Packet G → ADD → buffer: [B, D, E, F, G] → 5 readings → session complete
```

### Case 9: Session gap reset (watch removed)
```
Reading 1 at t=0
Watch removed. Next packet at t+5 min (gap > 4 min threshold)
  → Session auto-resets on arrival of next packet
  → New session starts with the new packet as reading 1
```

### Case 10: Error packets do not count toward session
```
Packet 1 → success → buffer: [reading 1]
Packet 2 → error (motion) → buffer unchanged: [reading 1]
Packet 3 → success → buffer: [reading 1, reading 2]
...continues until 5 successful readings
```

---

## Group 3 — NISO204 Per-Packet BP Mismatch Alert

### Case 11: NISO204 — AI and device agree (no alert)
```
AI estimates:   SBP=122, DBP=79
Device reports: BPSystolic=125, BPDiastolic=80

|125 − 122| = 3 mmHg ≤ 10 → OK
|80  − 79|  = 1 mmHg ≤ 10 → OK

→ bp_alert = null
```

### Case 12: NISO204 — SBP mismatch (alert triggered)
```
AI estimates:   SBP=122, DBP=79
Device reports: BPSystolic=138, BPDiastolic=81

|138 − 122| = 16 mmHg > 10 → MISMATCH

→ bp_alert = {
    type: "bp_mismatch",
    message: "Take manual BP reading — device and AI estimates disagree",
    device_bp: {sbp: 138, dbp: 81},
    ai_bp:     {sbp: 122, dbp: 79},
    diff_sbp: 16.0, diff_dbp: 2.0
  }
→ Session result still written with AI values
```

### Case 13: NISO204 — DBP mismatch only (alert triggered)
```
AI estimates:   SBP=120, DBP=72
Device reports: BPSystolic=122, BPDiastolic=84

|84 − 72| = 12 mmHg > 10 → MISMATCH (DBP alone is sufficient)
```

### Case 14: NISO204 — BP sentinel 404/200 (device did not measure this cycle)
```
Device reports: BPSystolic=404, BPDiastolic=200

404 > 250 → rejected as out-of-range sentinel
  → No comparison performed
  → AI estimate used as-is
  → bp_alert = null
```

### Case 15: NISO103 / NISO101 — BP in payload (ignored)
```
NISO103 packet contains bp.bpSystolic=138, bp.bpDiastolic=82
  → BP block is NOT read by the pipeline
  → No comparison, no alert
  → AI estimate used directly
  → bp_alert = null always for NISO103 and NISO101
```

### Case 16: LS06 — reference BP message received
```
LS06 packet arrives: bp.bpSystolic=128, bp.bpDiastolic=82, bpError=0
  → Device detected as LS06
  → plethWave is placeholder → inference skipped entirely
  → State machine transition runs (see Group 0)
  → Written to MongoDB as reference_bp doc

LS06 with bpError=1 or missing bp block:
  → Written as error doc: "LS06 reference BP: missing, invalid, or bpError set"
```

---

## Alert Priority in Session Document

When multiple alerts fire for the same session:

| Priority | Alert type | Trigger | Field |
|----------|-----------|---------|-------|
| 1 (highest) | `bp_drift` | Session avg drifts > ±10 mmHg from baseline (first breach) | `bp_alert` |
| 1 (highest) | `bp_drift_escalation` | 5 post-breach readings fail majority match → escalate | `bp_alert` |
| 1 (highest) | `breach_resolved` | Breach resolved — baseline updated | `bp_alert` |
| 2 | `bp_mismatch` | NISO204 device vs AI disagree per-packet > ±10 mmHg | `bp_alert` |
| 3 | `bp_trend` | Session-to-session change > ±15 mmHg | `bp_alert` (if no higher alert) or `bp_trend_alert` |

`bp_alert` always holds the most clinically significant alert. If a trend alert fires alongside a drift or mismatch alert, it is stored in `bp_trend_alert` so neither is lost.

**Drift detection is session-level only.** Individual AI packets are never checked for drift. Only completed 15-min session averages are compared against the baseline.

---

## Group 4 — MongoDB Output Document Shape

### Successful session (no alert):
```json
{
  "uuid":            "...",
  "admissionId":     "ADM001",
  "patientId":       "MRN001",
  "facilityId":      "CF001",
  "deviceId":        "NISO204-001",
  "timestamp":       1778214795000,
  "input": {
    "device_bp_present": true,
    "signal_source":     "Pleth",
    "source_hz":         120,
    "input_samples":     3600
  },
  "vitals": {
    "sbp":         122,
    "dbp":         79,
    "bp_category": "normal",
    "hb":          13.4,
    "glucose":     98.2
  },
  "offsets":          {"sbp": 0, "dbp": 0},
  "bp_alert":         null,
  "session_summary": {
    "total_readings":   5,
    "good_readings":    5,
    "outliers_removed": 0,
    "session_duration_minutes": 15
  },
  "processingStatus": "session_complete"
}
```

### Session with drift alert (normal state, session-level check):
```json
{
  "bp_alert": {
    "type":         "bp_drift",
    "message":      "BP has changed significantly from reference — take manual reading",
    "reference_bp": {"sbp": 150, "dbp": 95},
    "current_bp":   {"sbp": 168, "dbp": 108},
    "diff_sbp":     18.0,
    "diff_dbp":     13.0
  },
  "processingStatus": "session_complete"
}
```

### Breach escalation (case2_pending triggered):
```json
{
  "bp_alert": {
    "type":            "bp_drift_escalation",
    "message":         "BP readings inconsistent with reference — clinical review required",
    "pending_ref_bp":  {"sbp": 168, "dbp": 108},
    "match_count":     1,
    "total_readings":  5
  },
  "processingStatus": "session_complete"
}
```

### Breach resolved (session_complete after verification):
```json
{
  "bp_alert": {
    "type":          "breach_resolved",
    "message":       "BP calibration confirmed — reference baseline updated",
    "new_baseline":  {"sbp": 168, "dbp": 108},
    "offset":        {"sbp": 23, "dbp": 13}
  },
  "processingStatus": "session_complete"
}
```

### Immediate calibration snapshot (on first non-zero-offset confirmation):
```json
{
  "uuid":             "...",
  "admissionId":      "ADM001",
  "processingStatus": "calibration_applied",
  "vitals": {
    "sbp":         150,
    "dbp":         95,
    "bp_category": "stage2",
    "hb":          null,
    "glucose":     null
  },
  "offsets":  {"sbp": 23, "dbp": 13},
  "bp_alert": null
}
```
(Hb/Glucose are null here because this is a one-off snapshot, not a full session result.)

### Session with trend alert only (no_reference or unconfirmed state):
```json
{
  "bp_alert": {
    "type":        "bp_trend",
    "message":     "BP has changed significantly since last session — review recommended",
    "previous_bp": {"sbp": 122, "dbp": 79},
    "current_bp":  {"sbp": 142, "dbp": 82},
    "diff_sbp":    20.0,
    "diff_dbp":    3.0
  }
}
```

### Session with both drift and trend alerts:
```json
{
  "bp_alert": {
    "type": "bp_drift",
    "..."
  },
  "bp_trend_alert": {
    "type": "bp_trend",
    "..."
  }
}
```

### Reference BP document (LS06, with mismatch alert):
```json
{
  "uuid":             "...",
  "admissionId":      "ADM001",
  "processingStatus": "reference_bp",
  "reference_bp":     {"sbp": 150, "dbp": 95},
  "vitals":           null,
  "bp_alert": {
    "type":         "reference_mismatch",
    "message":      "Reference BP and AI estimates disagree — take another manual reading",
    "reference_bp": {"sbp": 150, "dbp": 95},
    "ai_bp":        {"sbp": 127, "dbp": 82},
    "diff_sbp":     23.0,
    "diff_dbp":     13.0
  }
}
```

### Error document:
```json
{
  "uuid":             "...",
  "admissionId":      "ADM001",
  "vitals":           null,
  "bp_alert":         null,
  "processingStatus": "error",
  "processingError":  "Insufficient data. Need at least 25s (3000 samples), but have 1200."
}
```

---

## Summary Table

### Group 0 — Initial Validation

| Case | Trigger | Action |
|------|---------|--------|
| 0a | First LS06 agrees with AI (≤ 10 mmHg) | normal, zero offset |
| 0b | First LS06 disagrees with AI (> 10 mmHg) | unconfirmed, reference_mismatch alert |
| 0c | First LS06, no AI reading yet | unconfirmed, no alert |
| 0d | Second LS06 ≈ AI (≤ 5 mmHg) | normal, zero offset (AI was correct) |
| 0e | Second LS06 force-confirms | normal, offset = LS06 − AI_raw; immediate snapshot if offset ≠ 0 |
| 0f | Session average drifts > 10 mmHg from baseline | breach_pending, bp_drift alert |
| 0g | New LS06 in normal state | update baseline only; offset unchanged; stay normal |
| 0h | AI packet, no LS06 ever | raw AI output, latest AI stored for future LS06 |

### Group 0T — Trend Detection

| Case | Trigger | Action |
|------|---------|--------|
| 0T-1 | First session | no trend alert |
| 0T-2 | Session change ≤ 15 mmHg | no trend alert |
| 0T-3 | Session change > 15 mmHg (SBP or DBP) | bp_trend alert |
| 0T-4 | Drift + trend both fire | bp_alert = drift/escalation/resolved, bp_trend_alert = trend |

### Group B — Breach Detection & Recovery

| Case | Trigger | Action |
|------|---------|--------|
| B1 | Session drift > 10 mmHg from baseline | breach_pending, bp_drift alert |
| B2 | LS06 arrives during breach_pending | pending_ref set, post_breach counter reset |
| B3 | 5 post-breach readings, ≥3 match pending_ref | breach_resolved, baseline = pending_ref, normal |
| B4 | 5 post-breach readings, <3 match pending_ref | case2_pending, bp_drift_escalation alert |
| B5 | Verification session completes, match pending_ref | breach_resolved, baseline = pending_ref, normal |
| B6 | Verification session completes, no match — cuff overrides | breach_resolved, cuff is truth, normal |
| B7 | New LS06 during breach_pending | pending_ref replaced, counter reset |
| B8 | New LS06 during case2_pending | pending_ref updated only |
| B9 | No LS06 during breach_pending | state waits indefinitely; sessions written, drift skipped |
| B10 | Session completes during breach_pending | written normally, drift check skipped |
| B11 | New LS06 in normal state | baseline updated, offset unchanged, stay normal |
| B12 | Session drift after breach resolved | new breach cycle from breach_pending |

### Groups 1–3 — Signal & Session

| Case | Trigger | Action |
|------|---------|--------|
| 1 | Signal < 25s | error doc, no session |
| 2 | All segments motion | skipped doc, no session |
| 3 | < 3 clean segments | error doc, no session |
| 4 | Partial motion | proceed with clean segments |
| 5 | Normal session | session_complete, bp_alert=null |
| 6 | Session outlier | outlier removed, session_complete |
| 7 | Too few good readings | session error, no write |
| 8 | Rapid re-send (< 3 min) | latest replaces earlier in buffer |
| 9 | Watch gap > 4 min | session resets |
| 10 | Error packet | not counted toward session |
| 11 | NISO204 agree (≤ 10 mmHg) | bp_alert=null |
| 12 | NISO204 SBP mismatch (> 10 mmHg) | bp_mismatch alert |
| 13 | NISO204 DBP mismatch (> 10 mmHg) | bp_mismatch alert |
| 14 | NISO204 BP absent/404/200 | bp_alert=null |
| 15 | NISO103/101 BP in payload | BP ignored, bp_alert=null always |
| 16 | LS06 message received | reference state machine runs |
