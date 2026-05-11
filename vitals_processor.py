"""
vitals_processor.py — Wrap vitals_standalone.process_vitals() for Kafka pipeline.

Device routing:
  NISO204  : flat JSON, Pleth field, 120 Hz. BPSystolic/BPDiastolic used for mismatch check.
             Sentinel 404/200 means BP not measured this cycle → skip check, use AI only.
  NISO103  : nested JSON, pleth.plethWave, 120 Hz. bp block ignored.
  NISO101  : nested JSON, pleth.plethWave, 100 Hz. bp block ignored.
  LS06     : reference BP device. bp.bpSystolic/bp.bpDiastolic extracted.
             plethWave is placeholder data — inference is skipped entirely.
             Returns a reference_bp document (no vitals).
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import vitals_standalone as _vs
import kafka_config as cfg

log = logging.getLogger(__name__)


def process(msg: dict) -> dict:
    """
    Process one Kafka message. Routes by device type.
    Always returns a MongoDB-ready dict. Never raises.
    """
    t_start = time.time()

    admission_id = msg.get("admissionId", msg.get("AdmissionId", "UNKNOWN"))
    patient_id   = msg.get("patientId",   msg.get("PatientId",   msg.get("PatId", "UNKNOWN")))
    facility_id  = msg.get("facilityId",  cfg.FACILITY_ID)
    device_id    = msg.get("deviceId",    msg.get("DeviceId",    msg.get("deviceID", "UNKNOWN")))
    timestamp    = msg.get("timestamp",   msg.get("epochTime",
                           int(datetime.now(timezone.utc).timestamp() * 1000)))

    # ── Device detection ──────────────────────────────────────────────────────
    device_type = _vs._detect_device(msg)

    # ── LS06: reference BP only, no pleth inference ───────────────────────────
    if device_type == _vs.DEVICE_LS06:
        return _process_reference_bp(msg, admission_id, patient_id, facility_id,
                                     device_id, timestamp)

    # ── Extract pleth metadata for logging (works for all non-LS06 devices) ──
    pleth_data, detected_hz = _vs._extract_pleth(msg, device_type)
    n_samples  = len(pleth_data) if pleth_data else 0
    source_hz  = msg.get("Source_HZ") or detected_hz or 120

    if device_type == _vs.DEVICE_NISO204:
        signal_source = "Pleth"
    elif device_type in (_vs.DEVICE_CHECKME, _vs.DEVICE_BERRYMED):
        signal_source = "pleth.plethWave"
    else:
        signal_source = "unknown"

    log.info(
        f"{admission_id} | device={device_type} signal={signal_source} "
        f"samples={n_samples} hz={source_hz}"
    )

    if n_samples < cfg.MIN_SAMPLES:
        log.warning(f"{admission_id} | Insufficient signal: {n_samples} samples (need {cfg.MIN_SAMPLES})")
        return _error_doc(
            admission_id, patient_id, facility_id, device_id, timestamp,
            f"Insufficient signal: {n_samples} samples (need {cfg.MIN_SAMPLES})"
        )

    # ── AI inference ──────────────────────────────────────────────────────────
    try:
        result = _vs.process_vitals(msg, source_hz=msg.get("Source_HZ"))
    except Exception as exc:
        log.error(f"{admission_id} | process_vitals raised: {exc}", exc_info=True)
        return _error_doc(admission_id, patient_id, facility_id, device_id, timestamp, str(exc))

    elapsed = round(time.time() - t_start, 2)

    if result.get("status") != "success":
        msg_txt = result.get("message", "Inference failed")
        log.warning(f"{admission_id} | Inference failed: {msg_txt}")
        return _error_doc(admission_id, patient_id, facility_id, device_id, timestamp, msg_txt)

    sbp      = result["sbp"]
    dbp      = result["dbp"]
    category = result["category"]
    hb       = result["hb"]
    glucose  = result["glucose"]
    offsets  = result.get("active_offsets", {})
    meta     = result.get("metadata", {})

    # ── NISO204 BP mismatch check ─────────────────────────────────────────────
    # BPSystolic/BPDiastolic are checked against the AI estimate.
    # Sentinel 404/200 → _extract_payload_bp returns (None, None) → no check.
    # NISO103 and NISO101 bp blocks are always ignored by _extract_payload_bp.
    bp_alert = None
    ref_sbp, ref_dbp = _vs._extract_payload_bp(msg, device_type)
    if ref_sbp is not None:
        diff_sbp = abs(ref_sbp - sbp)
        diff_dbp = abs(ref_dbp - dbp)
        if diff_sbp > cfg.BP_MISMATCH_THRESHOLD_SBP or diff_dbp > cfg.BP_MISMATCH_THRESHOLD_DBP:
            bp_alert = {
                "type":      "bp_mismatch",
                "message":   "Take manual BP reading — device and AI estimates disagree",
                "device_bp": {"sbp": ref_sbp, "dbp": ref_dbp},
                "ai_bp":     {"sbp": sbp,     "dbp": dbp},
                "diff_sbp":  round(diff_sbp, 1),
                "diff_dbp":  round(diff_dbp, 1),
            }
            log.warning(
                f"{admission_id} | BP mismatch: device={ref_sbp}/{ref_dbp} "
                f"AI={sbp}/{dbp} diff={diff_sbp:.1f}/{diff_dbp:.1f}"
            )

    log.info(
        f"{admission_id} | BP={sbp}/{dbp} ({category}) Hb={hb} Glu={glucose} {elapsed}s"
        + (f" ALERT:bp_mismatch" if bp_alert else "")
    )

    return {
        "uuid":        str(uuid.uuid4()),
        "admissionId": admission_id,
        "patientId":   patient_id,
        "facilityId":  facility_id,
        "deviceId":    device_id,
        "timestamp":   timestamp,

        "input": {
            "device_type":       device_type,
            "source_hz":         meta.get("source_hz", source_hz),
            "input_samples":     meta.get("input_samples", n_samples),
            "signal_source":     signal_source,
            "device_bp_present": ref_sbp is not None,
        },

        "vitals": {
            "sbp":         sbp,
            "dbp":         dbp,
            "bp_category": category,
            "hb":          hb,
            "glucose":     glucose,
        },

        "offsets":    offsets,
        "bp_alert":   bp_alert,

        "processingStatus":   None,
        "processedAt":        None,
        "processedBy":        None,
        "_processing_time_s": elapsed,
        "_processed_utc":     datetime.now(timezone.utc).isoformat(),
    }


def _process_reference_bp(msg, admission_id, patient_id, facility_id, device_id, timestamp):
    """
    Handle LS06 messages — reference BP cuff device.
    Extracts bp.bpSystolic / bp.bpDiastolic and returns a reference_bp document.
    No pleth inference is run (plethWave in LS06 payloads is placeholder data).
    """
    ref_sbp, ref_dbp = _vs._extract_payload_bp(msg, _vs.DEVICE_LS06)

    if ref_sbp is None:
        log.warning(f"{admission_id} | LS06 message with no valid BP (bpError or missing field)")
        return _error_doc(
            admission_id, patient_id, facility_id, device_id, timestamp,
            "LS06 reference BP: missing, invalid, or bpError set"
        )

    log.info(f"{admission_id} | LS06 reference BP received: {ref_sbp}/{ref_dbp}")

    return {
        "uuid":        str(uuid.uuid4()),
        "admissionId": admission_id,
        "patientId":   patient_id,
        "facilityId":  facility_id,
        "deviceId":    device_id,
        "timestamp":   timestamp,

        "input": {
            "device_type": _vs.DEVICE_LS06,
        },

        "reference_bp": {
            "sbp": ref_sbp,
            "dbp": ref_dbp,
        },

        "vitals":           None,
        "offsets":          {},
        "bp_alert":         None,
        "processingStatus": "reference_bp",
        "processedAt":      None,
        "processedBy":      None,
        "_processed_utc":   datetime.now(timezone.utc).isoformat(),
    }


def build_session_doc(last_doc: dict, session_result: dict) -> dict:
    """
    Replace the vitals in last_doc with the 15-min session-averaged values.
    Called by the consumer when SessionAggregator.is_ready() fires.
    """
    import copy
    doc = copy.deepcopy(last_doc)
    doc["vitals"] = {
        "sbp":         session_result["sbp"],
        "dbp":         session_result["dbp"],
        "bp_category": session_result["category"],
        "hb":          session_result["hb"],
        "glucose":     session_result["glucose"],
    }
    doc["session_summary"]  = session_result.get("session_summary", {})
    doc["processingStatus"] = "session_complete"
    return doc


def _error_doc(admission_id, patient_id, facility_id, device_id, timestamp, reason):
    return {
        "uuid":        str(uuid.uuid4()),
        "admissionId": admission_id,
        "patientId":   patient_id,
        "facilityId":  facility_id,
        "deviceId":    device_id,
        "timestamp":   timestamp,
        "input":       {},
        "vitals":      None,
        "offsets":     {},
        "bp_alert":    None,
        "processingStatus": "error",
        "processingError":  reason,
        "processedAt":      None,
        "processedBy":      None,
        "_processed_utc":   datetime.now(timezone.utc).isoformat(),
    }
