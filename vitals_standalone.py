import json
import sys
import os
import time
import argparse
import numpy as np

# Add current directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.append(script_dir)

try:
    import config as cfg
    from inference_engine import VitalInferenceEngine
except ImportError:
    sys.path.append(os.getcwd())
    try:
        import config as cfg
        from inference_engine import VitalInferenceEngine
    except ImportError as e:
        print(f"Error: Could not import inference_engine or config ({e}).")
        sys.exit(1)

DEVICE_NISO204  = "NISO204"
DEVICE_CHECKME  = "CHECKME"   # protocol: NISO103
DEVICE_BERRYMED = "BERRYMED"  # protocol: NISO101
DEVICE_LS06     = "LS06"      # reference BP cuff device — no pleth inference, BP only

# ── Motion gate thresholds ────────────────────────────────────────────────────
MOTION_THRESH_BERRYMED = 0.3   # g-units std from 1g gravity baseline
MOTION_THRESH_CHECKME  = 15    # raw byte units std

# ─────────────────────────────────────────────────────────────────────────────
# Device Detection
# ─────────────────────────────────────────────────────────────────────────────

def _detect_device(json_data):
    """Identify device type from JSON payload."""
    if json_data.get("DeviceName", "").upper() == "NISO204":
        return DEVICE_NISO204
    device_block = json_data.get("device", {}) or {}
    dtype = str(device_block.get("deviceType", "")).upper()
    if "CHECKME" in dtype or "NISO103" in dtype:
        return DEVICE_CHECKME
    if "BERRY" in dtype or "BERRYMED" in dtype or "NISO101" in dtype:
        return DEVICE_BERRYMED
    if "LS06" in dtype:
        return DEVICE_LS06
    # Fallback: infer from signal structure
    if json_data.get("Pleth"):        # NISO204 uses capital Pleth
        return DEVICE_NISO204
    if json_data.get("pleth"):
        # CHECKME has a nested bp block; BerryMed has acc (accelerometer)
        if json_data.get("bp") or (json_data.get("spo2") and not json_data.get("acc")):
            return DEVICE_CHECKME
        return DEVICE_BERRYMED
    return None


def _extract_payload_bp(json_data, device_type=None):
    """
    Extract reference BP from device payload.

    NISO204 : reads top-level BPSystolic / BPDiastolic.
              Sentinel 404/200 means device did not measure BP this packet
              → returns (None, None) so AI estimate is used without comparison.

    LS06    : reads bp.bpSystolic / bp.bpDiastolic (reference cuff device).
              plethWave in LS06 payloads is placeholder data — never used for inference.

    NISO103 / NISO101 : always returns (None, None) — their bp block is ignored.
    """
    if device_type is None:
        device_type = _detect_device(json_data)

    if device_type == DEVICE_NISO204:
        sbp = json_data.get("BPSystolic")
        dbp = json_data.get("BPDiastolic")
        if sbp is None or dbp is None:
            return None, None
        try:
            sbp, dbp = float(sbp), float(dbp)
        except (TypeError, ValueError):
            return None, None
        # 404/200 are the device sentinels for "BP not measured this cycle"
        if sbp < 60 or sbp > 250 or dbp < 40 or dbp > 150:
            return None, None
        return sbp, dbp

    if device_type == DEVICE_LS06:
        bp_block = json_data.get("bp", {}) or {}
        if not bp_block or bp_block.get("bpError", 0) != 0:
            return None, None
        sbp = bp_block.get("bpSystolic")
        dbp = bp_block.get("bpDiastolic")
        if sbp is None or dbp is None:
            return None, None
        try:
            sbp, dbp = float(sbp), float(dbp)
        except (TypeError, ValueError):
            return None, None
        if sbp < 60 or sbp > 250 or dbp < 40 or dbp > 150:
            return None, None
        return sbp, dbp

    # NISO103 / NISO101 — bp block intentionally ignored
    return None, None


def _bp_category(sbp, dbp):
    """Standard BP classification matching inference_engine output."""
    if sbp < 90 or dbp < 60:
        return "hypo"
    if sbp >= 130 or dbp >= 80:
        return "hyper"
    return "normal"


def _extract_pleth(json_data, device_type=None):
    """
    Returns (pleth_array, hz) based on device type.
      NISO204  : flat Pleth field, large ADC floats, 200 Hz native
      CHECKME  : nested pleth.plethWave, 0–168 range, 120 Hz
      BerryMed : nested pleth.plethWave, large ADC ints, 100 Hz
    """
    if device_type is None:
        device_type = _detect_device(json_data)

    if device_type == DEVICE_NISO204:
        pleth = json_data.get("Pleth", [])
        hz = json_data.get("FS") or 120
        return pleth, hz

    if device_type == DEVICE_CHECKME:
        nested = json_data.get("pleth", {}) or {}
        pleth = nested.get("plethWave") or nested.get("pleth", [])
        return pleth, 120

    if device_type == DEVICE_BERRYMED:
        nested = json_data.get("pleth", {}) or {}
        pleth = nested.get("plethWave") or nested.get("pleth", [])
        return pleth, 100

    if device_type == DEVICE_LS06:
        # LS06 is a reference BP cuff — plethWave is placeholder data, never for inference
        return [], None

    # Unknown fallback
    pleth = json_data.get("Pleth") or json_data.get("PlethWave", [])
    nested = json_data.get("pleth", {}) or {}
    pleth = pleth or nested.get("plethWave", [])
    return pleth, None


# ─────────────────────────────────────────────────────────────────────────────
# Device-Specific Signal Cleaning
# ─────────────────────────────────────────────────────────────────────────────

def _clean_checkme(pleth):
    """
    CHECKME O2: interpolate sentinel value 156 (0x9C = finger-off).
    Runs < 1s (<=120 samples): linear interpolation.
    Runs >= 1s (>120 samples): leave in place — noise gate will discard the segment.
    """
    arr = np.array(pleth, dtype=float)
    sentinel = 156
    i = 0
    while i < len(arr):
        if arr[i] == sentinel:
            j = i
            while j < len(arr) and arr[j] == sentinel:
                j += 1
            run_len = j - i
            if run_len > 120:
                # Long gap — leave for noise gate to discard segment
                i = j
                continue
            left_val  = arr[i - 1] if i > 0 else (arr[j] if j < len(arr) else 0)
            right_val = arr[j]     if j < len(arr) else left_val
            for k in range(i, j):
                t = (k - i + 1) / (run_len + 1)
                arr[k] = left_val + t * (right_val - left_val)
            i = j
        else:
            i += 1
    return arr.tolist()


def _clean_berrymed(pleth):
    """
    BerryMed: strip leading zeros (ADC startup artifact) then remove spikes.
    """
    arr = np.array(pleth, dtype=float)

    # Strip leading zeros
    first_nonzero = 0
    while first_nonzero < len(arr) and arr[first_nonzero] == 0:
        first_nonzero += 1
    arr = arr[first_nonzero:]

    if len(arr) == 0:
        return pleth  # all zeros — will fail min_samples check

    # Spike removal via local median filter
    WINDOW     = 11
    SPIKE_SIGMA = 5.0
    cleaned    = arr.copy()
    half       = WINDOW // 2
    for i in range(len(arr)):
        lo = max(0, i - half)
        hi = min(len(arr), i + half + 1)
        neighbourhood = arr[lo:hi]
        local_med = np.median(neighbourhood)
        local_std = np.std(neighbourhood)
        if local_std > 0 and abs(arr[i] - local_med) > SPIKE_SIGMA * local_std:
            cleaned[i] = local_med

    return cleaned.tolist()


def _clean_niso204(pleth):
    """NISO204: remove firmware-injected spike values (e.g. single corrupt sample mid-array)."""
    arr = np.array(pleth, dtype=float)
    WINDOW     = 11
    SPIKE_SIGMA = 5.0
    cleaned    = arr.copy()
    half       = WINDOW // 2
    for i in range(len(arr)):
        lo = max(0, i - half)
        hi = min(len(arr), i + half + 1)
        neighbourhood = arr[lo:hi]
        local_med = np.median(neighbourhood)
        local_std = np.std(neighbourhood)
        if local_std > 0 and abs(arr[i] - local_med) > SPIKE_SIGMA * local_std:
            cleaned[i] = local_med
    return cleaned.tolist()


def _clean_signal(pleth, device_type):
    """Route to the correct cleaning function for the device."""
    if device_type == DEVICE_CHECKME:
        return _clean_checkme(pleth)
    if device_type == DEVICE_BERRYMED:
        return _clean_berrymed(pleth)
    if device_type == DEVICE_NISO204:
        return _clean_niso204(pleth)
    return pleth


# ─────────────────────────────────────────────────────────────────────────────
# Accelerometer Motion Gate (per-segment)
# ─────────────────────────────────────────────────────────────────────────────

def _acc_to_motion_scores(acc_data):
    """
    Normalises any acc format to a 1D array of motion scores, one per PPG sample.
    Returns None if format is unrecognised.

    CHECKME  : flat [int, int, ...] scalar bytes 0–255
    BerryMed : [[x,y,z], [x,y,z], ...] g-units
    """
    if not acc_data:
        return None
    arr = np.array(acc_data, dtype=float)

    if arr.ndim == 2 and arr.shape[1] == 3:
        # BerryMed 3-axis: magnitude = sqrt(x²+y²+z²), rest ≈ 1.0g
        return np.linalg.norm(arr, axis=1)

    if arr.ndim == 1:
        if len(arr) % 3 == 0 and len(arr) >= 6:
            # Flat [x,y,z,x,y,z,...] — reshape and compute magnitude
            try:
                return np.linalg.norm(arr.reshape(-1, 3), axis=1)
            except Exception:
                pass
        # CHECKME scalar byte per sample — return as-is
        return arr

    return None


def _motion_mask_per_segment(motion_scores, num_segments, device_type):
    """
    Returns list of num_segments booleans.
    True  = segment has excessive motion → discard.
    False = clean → allow inference.

    Handles length mismatch between acc rate and PPG rate by scaling slice indices.
    """
    n = len(motion_scores)
    if n < 6:
        return [False] * num_segments  # too few acc samples to gate

    threshold = (MOTION_THRESH_BERRYMED
                 if device_type == DEVICE_BERRYMED
                 else MOTION_THRESH_CHECKME)

    seg_frac = 1.0 / num_segments
    mask = []
    for i in range(num_segments):
        lo = int(i * seg_frac * n)
        hi = int((i + 1) * seg_frac * n)
        seg = motion_scores[lo:hi]
        if len(seg) == 0:
            mask.append(False)
            continue

        if device_type == DEVICE_BERRYMED:
            # Deviation of magnitude from 1g baseline
            motion_val = float(np.std(seg - 1.0))
        else:
            motion_val = float(np.std(seg))

        mask.append(motion_val > threshold)

    return mask


# ─────────────────────────────────────────────────────────────────────────────
# Session Aggregator
# ─────────────────────────────────────────────────────────────────────────────

class SessionAggregator:
    """
    Accumulates per-watch-packet vitals results over a 15-minute session.
    Watch sends one packet every 3 minutes → 5 readings per session.
    Publishes averaged result when session is complete.
    """
    SESSION_GAP_S      = 240   # 4 min silence → auto-reset (3 min cadence + 1 min buffer)
    SESSION_DURATION_S = 900   # 15 min = 5 readings
    MIN_GOOD_READINGS  = 3     # need at least 3 of 5 to publish
    OUTLIER_THRESHOLD  = 10    # mmHg median deviation for outlier removal
    READING_MIN_GAP_S  = 180   # 3 min minimum gap between session readings (deduplication)

    def __init__(self):
        self.readings         = []
        self.session_start    = None
        self.last_packet_time = None

    def add_reading(self, result, packet_time=None):
        now = packet_time or time.time()
        # Auto-reset if watch was silent for > SESSION_GAP_S
        if self.last_packet_time and (now - self.last_packet_time) > self.SESSION_GAP_S:
            self.reset()
        self.last_packet_time = now
        if self.session_start is None:
            self.session_start = now
        if result.get("status") == "success":
            entry = {
                "sbp":      result["sbp"],
                "dbp":      result["dbp"],
                "hb":       result.get("hb", 0),
                "glucose":  result.get("glucose", 0),
                "category": result.get("category", "normal"),
                "ts":       now,
            }
            # 3-minute deduplication: if the last reading arrived within READING_MIN_GAP_S,
            # replace it with this newer one (manual override / rapid re-send case).
            if self.readings and (now - self.readings[-1]["ts"]) < self.READING_MIN_GAP_S:
                self.readings[-1] = entry
            else:
                self.readings.append(entry)

    def is_ready(self):
        if self.session_start is None or self.last_packet_time is None:
            return False
        time_done  = (self.last_packet_time - self.session_start) >= self.SESSION_DURATION_S
        count_done = len(self.readings) >= 5
        return time_done or count_done

    def get_session_result(self):
        M = len(self.readings)
        if M == 0:
            return {"status": "error", "message": "No successful readings in this session."}

        sbp_vals = [r["sbp"] for r in self.readings]
        dbp_vals = [r["dbp"] for r in self.readings]
        med_sbp  = float(np.median(sbp_vals))
        med_dbp  = float(np.median(dbp_vals))

        good = [r for r in self.readings
                if abs(r["sbp"] - med_sbp) <= self.OUTLIER_THRESHOLD
                and abs(r["dbp"] - med_dbp) <= self.OUTLIER_THRESHOLD]
        N = len(good)

        if N < self.MIN_GOOD_READINGS:
            return {
                "status":  "error",
                "message": f"Noisy data — only {N} of {M} readings were consistent. Need at least 3.",
                "session_summary": {
                    "total_readings":   M,
                    "good_readings":    N,
                    "outliers_removed": M - N,
                },
            }

        avg_sbp  = round(float(np.mean([r["sbp"] for r in good])), 1)
        avg_dbp  = round(float(np.mean([r["dbp"] for r in good])), 1)
        avg_hb   = round(float(np.mean([r["hb"]  for r in good])), 2)
        avg_glu  = round(float(np.mean([r["glucose"] for r in good])), 1)
        cats     = [r["category"] for r in good]
        category = max(set(cats), key=cats.count)

        return {
            "status":   "success",
            "sbp":      avg_sbp,
            "dbp":      avg_dbp,
            "bp":       f"{avg_sbp}/{avg_dbp}",
            "category": category,
            "hb":       avg_hb,
            "glucose":  avg_glu,
            "session_summary": {
                "total_readings":          M,
                "good_readings":           N,
                "outliers_removed":        M - N,
                "session_duration_minutes": 15,
            },
        }

    def reset(self):
        self.readings      = []
        self.session_start = None
        # Keep last_packet_time so next packet measures gap correctly


# ─────────────────────────────────────────────────────────────────────────────
# Engine singleton
# ─────────────────────────────────────────────────────────────────────────────

_engine: "VitalInferenceEngine | None" = None


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def process_vitals(json_data, source_hz=None):
    """
    Full pipeline: detect device → clean signal → motion gate → inference → return result.

    Returns dict with keys:
      status      : "success" | "error" | "skipped"
      sbp, dbp, bp, category, hb, glucose   (on success)
      active_offsets, metadata               (on success)
      message                                (on error/skipped)
    """
    global _engine
    if _engine is None:
        _engine = VitalInferenceEngine()
    engine = _engine

    # ── 1. Device detection ──────────────────────────────────────────────────
    device_type = _detect_device(json_data)
    print(f"DEBUG: Detected device type: {device_type}")

    # LS06 is a reference BP cuff — its plethWave is placeholder data.
    # AI inference must never run on LS06 data regardless of call path.
    if device_type == DEVICE_LS06:
        return {
            "status":  "error",
            "message": "LS06 is a reference BP device — pleth inference not allowed.",
        }

    # ── 2. Extract pleth + Hz ────────────────────────────────────────────────
    pleth_full, detected_hz = _extract_pleth(json_data, device_type)
    hz = source_hz or detected_hz or json_data.get("Source_HZ") or cfg.SAMPLING_RATE_HZ
    print(f"DEBUG: Processing stream at {hz} Hz")

    # ── 3. Minimum samples check ─────────────────────────────────────────────
    min_samples  = int(hz * 25)
    ideal_samples = int(hz * 30)

    if len(pleth_full) < min_samples:
        return {
            "status":  "error",
            "message": f"Insufficient data. Need at least 25s ({min_samples} samples), but have {len(pleth_full)}.",
        }

    # Window to last 30s
    pleth_raw = pleth_full[-ideal_samples:] if len(pleth_full) >= ideal_samples else list(pleth_full)
    input_samples = len(pleth_raw)

    # ── 4. Device-specific signal cleaning ───────────────────────────────────
    pleth = _clean_signal(pleth_raw, device_type)

    # ── 5. Per-segment accelerometer motion gate ──────────────────────────────
    motion_mask = [False] * 6   # default: no motion

    if device_type in (DEVICE_CHECKME,):  # BERRYMED accelerometer disabled for now
        acc_raw = None
        for candidate in ("acc", "accelerometer", "accel", "motion", "imu"):
            acc_raw = json_data.get(candidate)
            if acc_raw is None:
                acc_raw = (json_data.get("device") or {}).get(candidate)
            if acc_raw is not None:
                break

        # Handle new BerryMed format: accelerometer with x-axis, y-axis, z-axis dict
        if acc_raw is not None and isinstance(acc_raw, dict):
            x = acc_raw.get("x-axis") or acc_raw.get("x_axis") or acc_raw.get("x") or []
            y = acc_raw.get("y-axis") or acc_raw.get("y_axis") or acc_raw.get("y") or []
            z = acc_raw.get("z-axis") or acc_raw.get("z_axis") or acc_raw.get("z") or []
            if x and y and z and len(x) == len(y) == len(z):
                # Convert to 2D array format [[x,y,z], [x,y,z], ...]
                acc_raw = np.array(list(zip(x, y, z)), dtype=float)
            else:
                acc_raw = None

        if acc_raw is not None:
            motion_scores = _acc_to_motion_scores(acc_raw)
            if motion_scores is not None:
                motion_mask = _motion_mask_per_segment(motion_scores, 6, device_type)
                n_motion    = sum(motion_mask)
                print(f"DEBUG: Motion gate: {n_motion}/6 segments flagged")
                if all(motion_mask):
                    return {
                        "status":  "skipped",
                        "message": "Motion detected in all 6 segments — window discarded.",
                    }

    # ── 6. Patient metadata ───────────────────────────────────────────────────
    pr_data_full = json_data.get("PRAllData") or []
    # For CHECKME/BerryMed, fallback to spo2.pulseRate scalar
    if not pr_data_full:
        spo2_block = json_data.get("spo2", {}) or {}
        pr_val = spo2_block.get("pulseRate")
        if pr_val:
            pr_data_full = [pr_val]

    pr_data = pr_data_full[-30:] if len(pr_data_full) >= 30 else pr_data_full

    age    = json_data.get("Age", 35.0)
    gender = json_data.get("Gender", "Male")

    if json_data.get("BMI"):
        bmi = float(json_data["BMI"])
    elif json_data.get("Height") and json_data.get("Weight"):
        h_m = float(json_data["Height"]) / 100.0
        bmi = round(float(json_data["Weight"]) / (h_m ** 2), 1)
    else:
        bmi = 24.5

    # ── 7. Calibration offsets (pre-computed from Reference BP topic, carried forward by caller)
    # BP values from the PPG payload are never used here.
    # Offsets arrive ready-made via json_data["offsets"] when the caller has
    # already matched a Reference BP Kafka message to this device.
    offsets = json_data.get("offsets", {}).copy()

    # ── 8. Inference ─────────────────────────────────────────────────────────
    print("--- Running AI inference ---")
    results = engine.predict_vitals(
        ppg_segment    = pleth,
        actual_rate_hz = hz,
        age            = age,
        gender         = gender,
        bmi            = bmi,
        pr_all_data    = pr_data,
        offsets        = offsets if offsets else None,
        device_type    = device_type,
        motion_mask    = motion_mask,
    )

    if not results:
        return {"status": "error", "message": "Inference failed (poor signal quality or insufficient clean segments)."}

    # ── 9. Error propagation from engine ─────────────────────────────────────
    if results.get("_error"):
        v = results["valid"]; t = results["total"]; s = results["clean_seconds"]
        return {
            "status":  "error",
            "message": f"Only {v} of {t} segments were clean ({s}s). Need at least 15s of valid signal.",
        }

    # ── 10. Build output ──────────────────────────────────────────────────────
    sbp       = results.get("sbp")
    dbp       = results.get("dbp")
    category  = results.get("bp_category")
    valid_segs = results.get("valid_segments", 0)
    total_segs = results.get("total_segments", 6)

    return {
        "status":         "success",
        "sbp":            sbp,
        "dbp":            dbp,
        "bp":             f"{sbp}/{dbp}",
        "category":       category,
        "hb":             results.get("hb"),
        "glucose":        results.get("glucose"),
        "active_offsets": offsets,
        "metadata": {
            "device_type":      device_type,
            "source_hz":        hz,
            "valid_segments":   valid_segs,
            "total_segments":   total_segs,
            "signal_quality":   round(valid_segs / max(total_segs, 1), 2),
            "window_seconds":   30,
            "input_samples":    input_samples,
            "resampled_target": int(30 * 120),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Standalone Vitals Processor (30s Window Logic)")
    parser.add_argument("input_json", help="Path to input JSON")
    parser.add_argument("--hz",    type=float, help="Force source sampling rate (e.g. 100)")
    parser.add_argument("--output", help="Save result to separate JSON")
    args = parser.parse_args()

    if not os.path.exists(args.input_json):
        print(f"Error: {args.input_json} not found.")
        sys.exit(1)

    try:
        with open(args.input_json, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error: Could not parse JSON. {e}")
        sys.exit(1)

    print(f"\n--- Standalone Vitals Analysis: {args.input_json} ---")
    output = process_vitals(data, source_hz=args.hz)

    if output["status"] == "success":
        print("\n" + "=" * 38)
        print(f"  BLOOD PRESSURE : {output['bp']} mmHg")
        print(f"  CATEGORY       : {output['category']}")
        print(f"  HEMOGLOBIN     : {output['hb']} g/dL")
        print(f"  GLUCOSE        : {output['glucose']} mg/dL")
        m = output["metadata"]
        print(f"  SIGNAL QUALITY : {m['signal_quality']} ({m['valid_segments']}/{m['total_segments']} segments clean)")
        print(f"  DEVICE         : {m['device_type']} @ {m['source_hz']} Hz")
        print("=" * 38)
        if output["active_offsets"]:
            print(f"\nACTIVE OFFSETS: {json.dumps(output['active_offsets'])}")
        if args.output:
            with open(args.output, "w") as f:
                json.dump(output, f, indent=4)
            print(f"\nResults saved to: {args.output}")
    else:
        status = output["status"].upper()
        print(f"\n{status}: {output['message']}")


if __name__ == "__main__":
    main()
