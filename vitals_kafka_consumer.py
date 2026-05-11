"""
vitals_kafka_consumer.py — Consume PPG/reference data from Kafka, process, write to MongoDB.

Patient identification: keyed by admissionId from every Kafka message. Each patient's
state is completely independent — different patients on different devices never interact.

Reference validation + breach recovery state machine (per admissionId):

  no_reference   — No LS06 seen. Raw AI output shown as-is.
                   Trend alert if session change > ±15 mmHg.

  unconfirmed    — First LS06 arrived and disagreed with AI (> ±10 mmHg).
                   Raw AI still shown. Waiting for second LS06.
                   Second LS06 always force-confirms (two attempts max).
                   • Second LS06 ≈ AI (≤ LS06_CONFIRM_TOLERANCE): confirmed, zero offset
                   • Otherwise: confirmed, offset = LS06 − AI_raw

  normal         — Calibration active: output = AI_raw + offset.
                   Offset is set ONCE when first confirmed and NEVER changed again.
                   New LS06 → update baseline reference only, offset stays unchanged.
                   When newly confirmed with non-zero offset → immediate calibration
                   snapshot written to MongoDB so dashboard updates without waiting 15 min.
                   Session drift (session avg vs baseline) > ±10 mmHg → breach_pending.

  breach_pending — First drift alert sent. Waiting for:
                   (a) New LS06 to arrive as pending_ref
                   (b) 5 consecutive individual AI readings to compare against pending_ref
                   ≥3 of 5 match pending_ref (≤ ±10 mmHg): breach_resolved → normal.
                   <3 of 5 match: second alert → case2_pending. Session aggregator resets.
                   Sessions that complete during breach_pending are written but NOT drift-checked.

  case2_pending  — Second alert sent. Session aggregator has been reset.
                   Next valid 15-min session is the verification session.
                   Verification result (match or no match): LS06 is always clinical ground truth.
                   Baseline = pending_ref, offset unchanged → normal.
                   Error/skipped verification session → wait for next valid one.

Hb / Glucose: included in output whenever BP is present. Suppressed only if BP is absent.
Trend alert: fires in all states when session-to-session change > ±15 mmHg SBP or DBP.
3-min deduplication: if two pleth packets arrive within 3 min, the later one replaces the earlier
in the session buffer (handled by SessionAggregator).
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import time
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import kafka_config as cfg
from vitals_processor import process, build_session_doc, _error_doc
from vitals_mongo_writer import write_result
import vitals_standalone as vs

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, cfg.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-patient reference BP + breach recovery state  (keyed by admissionId)
# ---------------------------------------------------------------------------
_patient_refs: dict = {}
# Structure per admissionId:
#   {
#     "status":               str,         # "no_reference"|"unconfirmed"|"normal"|
#                                          #  "breach_pending"|"case2_pending"
#     "ref_sbp":              float|None,  # current baseline SBP (updates on new LS06 in normal)
#     "ref_dbp":              float|None,
#     "offset_sbp":           float|None,  # AI correction factor — set ONCE on confirm, never changed
#     "offset_dbp":           float|None,
#     "latest_ai_sbp":        float|None,  # most recent raw AI SBP (for LS06 comparison)
#     "latest_ai_dbp":        float|None,
#     "ls06_attempts":        int,         # counts first/second LS06 for initial validation
#     "pending_ref_sbp":      float|None,  # LS06 received during breach_pending/case2_pending
#     "pending_ref_dbp":      float|None,
#     "post_breach_readings": list,        # [{sbp, dbp}] calibrated readings in breach_pending window
#   }
_ref_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Per-patient last completed session (for trend detection)  (keyed by admissionId)
# ---------------------------------------------------------------------------
_patient_last_session: dict = {}
_trend_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Per-patient session aggregators  (keyed by admissionId)
# ---------------------------------------------------------------------------
_device_sessions: dict = {}
_session_lock = threading.Lock()


def _get_aggregator(admission_id: str) -> vs.SessionAggregator:
    if admission_id not in _device_sessions:
        _device_sessions[admission_id] = vs.SessionAggregator()
    return _device_sessions[admission_id]


def _reset_session(admission_id: str):
    with _session_lock:
        _device_sessions[admission_id] = vs.SessionAggregator()


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_running = True


def _handle_sigterm(signum, frame):
    global _running
    log.info("SIGTERM received — finishing in-flight messages, then shutting down.")
    _running = False


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)


# ---------------------------------------------------------------------------
# Kafka message parser
# ---------------------------------------------------------------------------

def _parse(raw: bytes) -> dict | None:
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:
        log.warning(f"Failed to parse Kafka message: {exc}")
        return None


# ---------------------------------------------------------------------------
# Reference BP state machine
# ---------------------------------------------------------------------------

def _on_ls06_arrived(admission_id: str, ls06_sbp: float, ls06_dbp: float):
    """
    Process an incoming LS06 reference reading and advance the patient state machine.

    Returns:
      (bp_alert_or_None, newly_confirmed: bool)
      bp_alert   — alert to embed in the reference_bp document (or None)
      newly_confirmed — True when transitioning to normal from no_reference/unconfirmed
                        AND offset is non-zero (dashboard needs immediate snapshot)
    """
    with _ref_lock:
        entry          = _patient_refs.get(admission_id)
        current_status = entry["status"] if entry else "no_reference"
        latest_ai_sbp  = (entry or {}).get("latest_ai_sbp")
        latest_ai_dbp  = (entry or {}).get("latest_ai_dbp")
        ls06_attempts  = (entry or {}).get("ls06_attempts", 0)
        old_offset_sbp = (entry or {}).get("offset_sbp")
        old_offset_dbp = (entry or {}).get("offset_dbp")

    bp_alert      = None
    new_status    = "unconfirmed"
    offset_sbp    = old_offset_sbp   # preserved unless being set for the first time
    offset_dbp    = old_offset_dbp
    newly_confirmed = False

    if current_status == "no_reference":
        if latest_ai_sbp is not None:
            diff_sbp = abs(ls06_sbp - latest_ai_sbp)
            diff_dbp = abs(ls06_dbp - latest_ai_dbp)
            if diff_sbp <= cfg.BP_MISMATCH_THRESHOLD_SBP and diff_dbp <= cfg.BP_MISMATCH_THRESHOLD_DBP:
                new_status    = "normal"
                offset_sbp    = 0.0
                offset_dbp    = 0.0
                newly_confirmed = False  # zero offset — AI was right, no snapshot needed
                log.info(
                    f"{admission_id} | LS06 agrees with AI → normal (zero offset) "
                    f"LS06={ls06_sbp}/{ls06_dbp} AI={latest_ai_sbp}/{latest_ai_dbp}"
                )
            else:
                new_status = "unconfirmed"
                bp_alert = {
                    "type":         "reference_mismatch",
                    "message":      "Reference BP and AI estimates disagree — take another manual reading",
                    "reference_bp": {"sbp": ls06_sbp,      "dbp": ls06_dbp},
                    "ai_bp":        {"sbp": latest_ai_sbp, "dbp": latest_ai_dbp},
                    "diff_sbp":     round(diff_sbp, 1),
                    "diff_dbp":     round(diff_dbp, 1),
                }
                log.warning(
                    f"{admission_id} | LS06 vs AI mismatch → unconfirmed "
                    f"LS06={ls06_sbp}/{ls06_dbp} AI={latest_ai_sbp}/{latest_ai_dbp}"
                )
        else:
            new_status = "unconfirmed"
            log.info(f"{admission_id} | First LS06 {ls06_sbp}/{ls06_dbp}, no AI yet → unconfirmed")

    elif current_status == "unconfirmed":
        # Second LS06 — force-confirm regardless
        if latest_ai_sbp is not None:
            diff_sbp = abs(ls06_sbp - latest_ai_sbp)
            diff_dbp = abs(ls06_dbp - latest_ai_dbp)
            if diff_sbp <= cfg.LS06_CONFIRM_TOLERANCE and diff_dbp <= cfg.LS06_CONFIRM_TOLERANCE:
                offset_sbp = 0.0
                offset_dbp = 0.0
                newly_confirmed = False
                log.info(
                    f"{admission_id} | 2nd LS06 matches AI within tolerance → normal (zero offset)"
                )
            else:
                offset_sbp = round(ls06_sbp - latest_ai_sbp, 1)
                offset_dbp = round(ls06_dbp - latest_ai_dbp, 1)
                newly_confirmed = True   # non-zero offset → write immediate snapshot
                log.info(
                    f"{admission_id} | 2nd LS06 force-confirms → normal "
                    f"offset={offset_sbp:+}/{offset_dbp:+}"
                )
        else:
            offset_sbp = 0.0
            offset_dbp = 0.0
            newly_confirmed = False
            log.info(f"{admission_id} | 2nd LS06 {ls06_sbp}/{ls06_dbp}, no AI yet → normal (zero offset)")
        new_status = "normal"

    elif current_status == "normal":
        # New LS06 during normal monitoring → update baseline only, offset UNCHANGED
        new_status = "normal"
        # offset_sbp/offset_dbp already preserved from old entry
        log.info(
            f"{admission_id} | New LS06 in normal → baseline updated "
            f"{(entry or {}).get('ref_sbp')}/{(entry or {}).get('ref_dbp')} "
            f"→ {ls06_sbp}/{ls06_dbp} | offset unchanged"
        )

    elif current_status == "breach_pending":
        # New LS06 during breach → becomes the pending reference, reset comparison counter
        new_status = "breach_pending"
        # Don't change ref/offset here — just set pending_ref (handled below)
        log.info(
            f"{admission_id} | New LS06 during breach_pending → pending_ref={ls06_sbp}/{ls06_dbp}, counter reset"
        )

    else:
        # case2_pending — update pending_ref for upcoming verification session
        new_status = "case2_pending"
        log.info(
            f"{admission_id} | New LS06 during case2_pending → pending_ref updated to {ls06_sbp}/{ls06_dbp}"
        )

    # Build the updated state entry
    with _ref_lock:
        existing = _patient_refs.get(admission_id, {})

        if current_status in ("breach_pending", "case2_pending"):
            # In breach states: LS06 goes to pending_ref, NOT the baseline
            _patient_refs[admission_id] = {
                **existing,
                "status":               new_status,
                "pending_ref_sbp":      ls06_sbp,
                "pending_ref_dbp":      ls06_dbp,
                "post_breach_readings": [],   # restart comparison counter
                "ls06_attempts":        ls06_attempts + 1,
            }
        else:
            # In no_reference / unconfirmed / normal: LS06 updates the baseline
            _patient_refs[admission_id] = {
                "status":               new_status,
                "ref_sbp":              ls06_sbp,
                "ref_dbp":              ls06_dbp,
                "offset_sbp":          offset_sbp,
                "offset_dbp":          offset_dbp,
                "latest_ai_sbp":        existing.get("latest_ai_sbp"),
                "latest_ai_dbp":        existing.get("latest_ai_dbp"),
                "ls06_attempts":        ls06_attempts + 1,
                "pending_ref_sbp":      existing.get("pending_ref_sbp"),
                "pending_ref_dbp":      existing.get("pending_ref_dbp"),
                "post_breach_readings": existing.get("post_breach_readings", []),
            }

    return bp_alert, newly_confirmed


def _apply_reference_calibration(admission_id: str, raw_sbp: float, raw_dbp: float):
    """
    Apply stored offset to raw AI values.

    no_reference / unconfirmed → raw values unchanged (no offset yet).
    normal / breach_pending / case2_pending → apply offset (calibration stays active during breach).

    Also always updates latest_ai_sbp/dbp so the next LS06 comparison has current data.

    Returns: (cal_sbp, cal_dbp, offsets_dict)
    Note: NO per-packet drift check here. Drift is checked at session level only.
    """
    with _ref_lock:
        entry = _patient_refs.get(admission_id)
        if entry is not None:
            entry["latest_ai_sbp"] = raw_sbp
            entry["latest_ai_dbp"] = raw_dbp
        else:
            _patient_refs[admission_id] = {
                "status":               "no_reference",
                "ref_sbp":              None,
                "ref_dbp":              None,
                "offset_sbp":           None,
                "offset_dbp":           None,
                "latest_ai_sbp":        raw_sbp,
                "latest_ai_dbp":        raw_dbp,
                "ls06_attempts":        0,
                "pending_ref_sbp":      None,
                "pending_ref_dbp":      None,
                "post_breach_readings": [],
            }
            entry = _patient_refs[admission_id]

        status     = entry["status"]
        offset_sbp = entry["offset_sbp"]
        offset_dbp = entry["offset_dbp"]

    if status in ("no_reference", "unconfirmed") or offset_sbp is None:
        return raw_sbp, raw_dbp, {}

    cal_sbp = round(raw_sbp + offset_sbp, 1)
    cal_dbp = round(raw_dbp + offset_dbp, 1)
    return cal_sbp, cal_dbp, {"sbp": offset_sbp, "dbp": offset_dbp}


def _record_post_breach_reading(admission_id: str, cal_sbp: float, cal_dbp: float):
    """
    Called after every individual AI reading when status == "breach_pending".
    Accumulates readings against the pending LS06 reference.

    Returns:
      (alert_or_None, reset_session: bool)
      alert        — bp_alert dict to embed in the next session doc (None = nothing)
      reset_session — True when escalating to case2_pending (aggregator must be reset)
    """
    with _ref_lock:
        entry = _patient_refs.get(admission_id, {})
        if entry.get("status") != "breach_pending":
            return None, False

        pending_sbp = entry.get("pending_ref_sbp")
        pending_dbp = entry.get("pending_ref_dbp")

        if pending_sbp is None:
            # Waiting for LS06 to arrive — don't count yet
            return None, False

        readings = entry.get("post_breach_readings", [])
        if len(readings) < 5:
            readings.append({"sbp": cal_sbp, "dbp": cal_dbp})
            entry["post_breach_readings"] = readings

        if len(readings) < 5:
            log.info(
                f"{admission_id} | Post-breach reading {len(readings)}/5: "
                f"{cal_sbp}/{cal_dbp} vs pending {pending_sbp}/{pending_dbp}"
            )
            return None, False

        # 5 readings collected — evaluate majority match
        matches = sum(
            1 for r in readings
            if abs(r["sbp"] - pending_sbp) <= cfg.BP_MISMATCH_THRESHOLD_SBP
            and abs(r["dbp"] - pending_dbp) <= cfg.BP_MISMATCH_THRESHOLD_DBP
        )
        log.info(
            f"{admission_id} | Post-breach 5/5 done: {matches}/5 match "
            f"pending_ref={pending_sbp}/{pending_dbp}"
        )

        if matches >= 3:
            # Breach resolved — update baseline to pending_ref, offset unchanged
            old_ref_sbp = entry.get("ref_sbp")
            old_ref_dbp = entry.get("ref_dbp")
            _patient_refs[admission_id] = {
                **entry,
                "status":               "normal",
                "ref_sbp":              pending_sbp,
                "ref_dbp":              pending_dbp,
                "pending_ref_sbp":      None,
                "pending_ref_dbp":      None,
                "post_breach_readings": [],
            }
            log.info(
                f"{admission_id} | Breach resolved ({matches}/5 match) — "
                f"baseline {old_ref_sbp}/{old_ref_dbp} → {pending_sbp}/{pending_dbp}"
            )
            alert = {
                "type":    "breach_resolved",
                "message": "BP has stabilised at new reference level — monitoring resumed",
                "old_baseline":  {"sbp": old_ref_sbp, "dbp": old_ref_dbp},
                "new_baseline":  {"sbp": pending_sbp,  "dbp": pending_dbp},
                "readings_matched": matches,
            }
            return alert, False
        else:
            # <3 match — escalate to case2_pending
            _patient_refs[admission_id] = {
                **entry,
                "status":               "case2_pending",
                "post_breach_readings": [],
            }
            log.warning(
                f"{admission_id} | Breach unresolved ({matches}/5 match) — "
                f"escalating to case2_pending, session aggregator reset"
            )
            alert = {
                "type":    "bp_drift_escalation",
                "message": "BP discrepancy persists — please verify manually",
                "pending_reference": {"sbp": pending_sbp, "dbp": pending_dbp},
                "readings_matched":  matches,
            }
            return alert, True   # caller must reset session aggregator


def _check_session_drift(admission_id: str, session_sbp: float, session_dbp: float):
    """
    Called after every completed session.

    normal        → compare session avg vs baseline; breach if > threshold
    breach_pending → skip drift check (session is written but not re-evaluated)
    case2_pending  → this IS the verification session; accept pending_ref as truth
    no_reference / unconfirmed → no check

    Returns:
      (alert_or_None, is_verification_complete: bool)
    """
    with _ref_lock:
        entry = _patient_refs.get(admission_id, {})
        status      = entry.get("status", "no_reference")
        ref_sbp     = entry.get("ref_sbp")
        ref_dbp     = entry.get("ref_dbp")
        pending_sbp = entry.get("pending_ref_sbp")
        pending_dbp = entry.get("pending_ref_dbp")

    if status in ("no_reference", "unconfirmed") or ref_sbp is None:
        return None, False

    if status == "normal":
        diff_sbp = abs(session_sbp - ref_sbp)
        diff_dbp = abs(session_dbp - ref_dbp)
        if diff_sbp > cfg.BP_MISMATCH_THRESHOLD_SBP or diff_dbp > cfg.BP_MISMATCH_THRESHOLD_DBP:
            with _ref_lock:
                _patient_refs[admission_id] = {
                    **_patient_refs.get(admission_id, {}),
                    "status":               "breach_pending",
                    "pending_ref_sbp":      None,
                    "pending_ref_dbp":      None,
                    "post_breach_readings": [],
                }
            alert = {
                "type":         "bp_drift",
                "message":      "BP has changed significantly from reference — take manual reading",
                "reference_bp": {"sbp": ref_sbp,     "dbp": ref_dbp},
                "current_bp":   {"sbp": session_sbp, "dbp": session_dbp},
                "diff_sbp":     round(diff_sbp, 1),
                "diff_dbp":     round(diff_dbp, 1),
            }
            log.warning(
                f"{admission_id} | Session drift → breach_pending: "
                f"session={session_sbp}/{session_dbp} ref={ref_sbp}/{ref_dbp} "
                f"diff={diff_sbp:.1f}/{diff_dbp:.1f}"
            )
            return alert, False
        return None, False

    if status == "breach_pending":
        # Session completed during breach investigation — write but don't re-alert
        log.info(
            f"{admission_id} | Session completed during breach_pending — drift check skipped"
        )
        return None, False

    if status == "case2_pending":
        # Verification session — accept pending_ref as clinical ground truth regardless
        if pending_sbp is None:
            log.warning(f"{admission_id} | case2_pending verification session but no pending_ref — keeping state")
            return None, False

        old_ref_sbp = ref_sbp
        old_ref_dbp = ref_dbp
        diff_sbp    = abs(session_sbp - pending_sbp)
        diff_dbp    = abs(session_dbp - pending_dbp)
        matched     = diff_sbp <= cfg.BP_MISMATCH_THRESHOLD_SBP and diff_dbp <= cfg.BP_MISMATCH_THRESHOLD_DBP

        with _ref_lock:
            _patient_refs[admission_id] = {
                **_patient_refs.get(admission_id, {}),
                "status":               "normal",
                "ref_sbp":              pending_sbp,
                "ref_dbp":              pending_dbp,
                "pending_ref_sbp":      None,
                "pending_ref_dbp":      None,
                "post_breach_readings": [],
            }
        resolution = "session_confirmed" if matched else "cuff_override"
        log.info(
            f"{admission_id} | Verification session complete ({resolution}) — "
            f"baseline {old_ref_sbp}/{old_ref_dbp} → {pending_sbp}/{pending_dbp}"
        )
        alert = {
            "type":         "breach_resolved",
            "message":      "BP reference verified — monitoring resumed at new baseline",
            "old_baseline": {"sbp": old_ref_sbp,  "dbp": old_ref_dbp},
            "new_baseline": {"sbp": pending_sbp,   "dbp": pending_dbp},
            "resolution":   resolution,
        }
        return alert, True

    return None, False


def _check_trend(admission_id: str, session_sbp: float, session_dbp: float) -> dict | None:
    """
    Fires in ALL states when session-to-session change exceeds ±15 mmHg SBP or DBP.
    Either SBP or DBP exceeding the threshold is sufficient to trigger the alert.
    """
    with _trend_lock:
        prev = _patient_last_session.get(admission_id)
        _patient_last_session[admission_id] = {"sbp": session_sbp, "dbp": session_dbp}

    if prev is None:
        return None

    diff_sbp = abs(session_sbp - prev["sbp"])
    diff_dbp = abs(session_dbp - prev["dbp"])
    if diff_sbp > cfg.BP_TREND_THRESHOLD_SBP or diff_dbp > cfg.BP_TREND_THRESHOLD_DBP:
        alert = {
            "type":        "bp_trend",
            "message":     "BP has changed significantly since last session — review recommended",
            "previous_bp": {"sbp": prev["sbp"],  "dbp": prev["dbp"]},
            "current_bp":  {"sbp": session_sbp,  "dbp": session_dbp},
            "diff_sbp":    round(diff_sbp, 1),
            "diff_dbp":    round(diff_dbp, 1),
        }
        log.warning(
            f"{admission_id} | Trend alert: prev={prev['sbp']}/{prev['dbp']} "
            f"current={session_sbp}/{session_dbp}"
        )
        return alert
    return None


def _build_calibration_snapshot(admission_id: str, ref_doc: dict) -> dict | None:
    """
    Build an immediate snapshot document written to MongoDB the moment a non-zero
    calibration offset is confirmed. Lets the dashboard show the corrected BP without
    waiting up to 15 minutes for the next session to complete.

    Only written when transitioning from unconfirmed → normal with a non-zero offset.
    """
    with _ref_lock:
        entry = _patient_refs.get(admission_id)
    if not entry or entry["status"] != "normal":
        return None
    offset_sbp    = entry.get("offset_sbp")
    offset_dbp    = entry.get("offset_dbp")
    latest_ai_sbp = entry.get("latest_ai_sbp")
    latest_ai_dbp = entry.get("latest_ai_dbp")

    if offset_sbp is None or latest_ai_sbp is None:
        return None
    if offset_sbp == 0.0 and offset_dbp == 0.0:
        return None   # zero offset — AI was already correct; no snapshot needed

    cal_sbp = round(latest_ai_sbp + offset_sbp, 1)
    cal_dbp = round(latest_ai_dbp + offset_dbp, 1)

    return {
        "uuid":             str(uuid.uuid4()),
        "admissionId":      admission_id,
        "patientId":        ref_doc.get("patientId",  "UNKNOWN"),
        "facilityId":       ref_doc.get("facilityId", cfg.FACILITY_ID),
        "deviceId":         ref_doc.get("deviceId",   "UNKNOWN"),
        "timestamp":        ref_doc.get("timestamp"),
        "processingStatus": "calibration_applied",
        "vitals": {
            "sbp":         cal_sbp,
            "dbp":         cal_dbp,
            "bp_category": vs._bp_category(cal_sbp, cal_dbp),
            "hb":          None,   # no session average yet
            "glucose":     None,
        },
        "offsets":          {"sbp": offset_sbp, "dbp": offset_dbp},
        "bp_alert":         None,
        "_processed_utc":   datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Message validation
# ---------------------------------------------------------------------------

def _validate(msg: dict) -> bool:
    device_type = vs._detect_device(msg)
    if device_type == vs.DEVICE_LS06:
        return True
    nested    = (msg.get("pleth") or {})
    has_pleth = bool(msg.get("Pleth")) or bool(nested.get("plethWave"))
    if not has_pleth:
        log.warning(
            f"{msg.get('admissionId', '?')} | {device_type}: missing pleth — rejected"
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Message handling
# ---------------------------------------------------------------------------

def _handle_message(msg_dict: dict):
    """
    Route one Kafka message. Never raises.

    Patients are identified by admissionId. All per-patient state (reference, session,
    calibration, breach recovery) is keyed by admissionId so different patients never mix.
    """
    admission_id = msg_dict.get("admissionId", "UNKNOWN")
    device_id    = msg_dict.get("deviceId", msg_dict.get("DeviceId", msg_dict.get("deviceID", "UNKNOWN")))
    timestamp_ms = msg_dict.get("timestamp", msg_dict.get("epochTime",
                                int(datetime.now(timezone.utc).timestamp() * 1000)))
    packet_time  = timestamp_ms / 1000.0

    # ── 1. Run inference (or reference extraction for LS06) ───────────────────
    try:
        doc = process(msg_dict)
    except Exception as exc:
        log.error(f"{admission_id} | process() error: {exc}", exc_info=True)
        try:
            write_result(_error_doc(
                admission_id,
                msg_dict.get("patientId", "UNKNOWN"),
                msg_dict.get("facilityId", cfg.FACILITY_ID),
                device_id, timestamp_ms, str(exc),
            ))
        except Exception:
            pass
        return

    status = doc.get("processingStatus")

    # ── 2. LS06 → state machine transition ────────────────────────────────────
    if status == "reference_bp":
        ref_bp = doc.get("reference_bp", {})
        if ref_bp:
            ref_alert, newly_confirmed = _on_ls06_arrived(
                admission_id, ref_bp["sbp"], ref_bp["dbp"]
            )
            if ref_alert:
                doc["bp_alert"] = ref_alert
        else:
            newly_confirmed = False

        write_result(doc)

        # Immediate calibration snapshot if first confirmation with non-zero offset
        if newly_confirmed:
            snap = _build_calibration_snapshot(admission_id, doc)
            if snap:
                write_result(snap)
                log.info(
                    f"{admission_id} | Calibration snapshot written: "
                    f"BP={snap['vitals']['sbp']}/{snap['vitals']['dbp']}"
                )
        return

    # ── 3. Error / skipped → write and stop ───────────────────────────────────
    if status in ("error", "skipped") or doc.get("vitals") is None:
        write_result(doc)
        return

    # ── 4. Apply reference calibration (offset only — no per-packet drift) ───
    vitals  = doc["vitals"]
    raw_sbp = vitals["sbp"]
    raw_dbp = vitals["dbp"]

    cal_sbp, cal_dbp, offsets = _apply_reference_calibration(admission_id, raw_sbp, raw_dbp)

    doc["vitals"]["sbp"]         = cal_sbp
    doc["vitals"]["dbp"]         = cal_dbp
    doc["vitals"]["bp_category"] = vs._bp_category(cal_sbp, cal_dbp)
    doc["offsets"]               = offsets

    # Hb / Glucose: only present when BP is present. If BP is None (shouldn't happen
    # here since we only reach this point on a successful inference result), suppress.
    if cal_sbp is None:
        doc["vitals"]["hb"]      = None
        doc["vitals"]["glucose"] = None

    log.info(f"{admission_id} | BP={cal_sbp}/{cal_dbp} raw={raw_sbp}/{raw_dbp}")

    # ── 5. Post-breach individual reading tracking ────────────────────────────
    breach_alert     = None
    reset_session    = False
    with _ref_lock:
        current_status = (_patient_refs.get(admission_id) or {}).get("status", "no_reference")

    if current_status == "breach_pending":
        breach_alert, reset_session = _record_post_breach_reading(admission_id, cal_sbp, cal_dbp)
        if reset_session:
            _reset_session(admission_id)
            # Don't feed this reading into the (just-reset) verification aggregator
            if breach_alert:
                doc["bp_alert"] = breach_alert
            write_result(doc)
            log.warning(f"{admission_id} | Escalated to case2_pending — session aggregator reset")
            return

    # ── 6. Feed calibrated reading into session aggregator ────────────────────
    reading = {
        "status":   "success",
        "sbp":      cal_sbp,
        "dbp":      cal_dbp,
        "hb":       vitals.get("hb") if vitals.get("hb") is not None else 0,
        "glucose":  vitals.get("glucose") if vitals.get("glucose") is not None else 0,
        "category": doc["vitals"]["bp_category"],
    }

    with _session_lock:
        agg = _get_aggregator(admission_id)
        agg.add_reading(reading, packet_time=packet_time)
        n = len(agg.readings)

        if not agg.is_ready():
            log.info(f"{admission_id} | Session {n}/5 — waiting")
            return

        session_result = agg.get_session_result()
        agg.reset()

    if session_result.get("status") != "success":
        log.warning(f"{admission_id} | Session failed: {session_result.get('message')}")
        return

    sess_sbp = session_result["sbp"]
    sess_dbp = session_result["dbp"]

    # ── 7. Session-level drift check ──────────────────────────────────────────
    drift_alert, verification_complete = _check_session_drift(admission_id, sess_sbp, sess_dbp)

    # ── 8. Trend check (all states) ───────────────────────────────────────────
    trend_alert = _check_trend(admission_id, sess_sbp, sess_dbp)

    final_doc = build_session_doc(doc, session_result)

    # Alert priority: breach_resolved / bp_drift > NISO204 bp_mismatch > bp_trend
    top_alert = drift_alert or breach_alert or doc.get("bp_alert")
    final_doc["bp_alert"] = top_alert
    if trend_alert:
        if top_alert:
            final_doc["bp_trend_alert"] = trend_alert
        else:
            final_doc["bp_alert"] = trend_alert

    write_result(final_doc)
    log.info(
        f"{admission_id} | Session complete → BP={sess_sbp}/{sess_dbp} "
        f"({session_result['category']}) written"
        + (f" | ALERT: {final_doc['bp_alert']['type']}" if final_doc.get("bp_alert") else "")
        + (" | VERIFICATION COMPLETE" if verification_complete else "")
    )


# ---------------------------------------------------------------------------
# Consumer loop
# ---------------------------------------------------------------------------

def run():
    """
    Single-threaded Kafka consumer.

    Processes one message at a time — safe, simple, and correct.
    Each patient sends ~1 packet per 3 minutes, so throughput is never a bottleneck
    for a typical ward of patients. For higher throughput, run multiple consumer
    instances (different group IDs or partitioned topic) rather than adding threads.
    """
    conf = {
        "bootstrap.servers":    cfg.KAFKA_BOOTSTRAP_SERVERS,
        "group.id":             cfg.KAFKA_GROUP_ID,
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   False,
        # Give inference plenty of time before the broker considers the consumer dead
        "max.poll.interval.ms": 300000,   # 5 min
        "session.timeout.ms":   30000,
        "heartbeat.interval.ms": 10000,
    }

    consumer = Consumer(conf)
    consumer.subscribe([cfg.KAFKA_TOPIC])

    log.info(
        f"PPG Vitals Consumer started (single-threaded) | "
        f"topic={cfg.KAFKA_TOPIC} group={cfg.KAFKA_GROUP_ID} "
        f"bootstrap={cfg.KAFKA_BOOTSTRAP_SERVERS}"
    )

    try:
        while _running:
            kafka_msg = consumer.poll(timeout=1.0)

            if kafka_msg is None:
                continue

            if kafka_msg.error():
                code = kafka_msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    log.debug(
                        f"Partition EOF — topic={kafka_msg.topic()} "
                        f"partition={kafka_msg.partition()} offset={kafka_msg.offset()}"
                    )
                else:
                    log.error(f"Kafka consumer error: {kafka_msg.error()}")
                consumer.commit(message=kafka_msg, asynchronous=True)
                continue

            msg_dict = _parse(kafka_msg.value())
            if msg_dict is not None and _validate(msg_dict):
                try:
                    _handle_message(msg_dict)
                except Exception as exc:
                    log.error(
                        f"Unhandled error processing message "
                        f"(admissionId={msg_dict.get('admissionId', '?')}): {exc}",
                        exc_info=True,
                    )

            # Always commit — even on parse/validate failure, so we don't re-process
            consumer.commit(message=kafka_msg, asynchronous=True)

    except KafkaException as exc:
        log.error(f"Fatal Kafka error: {exc}")
        sys.exit(1)
    finally:
        consumer.close()
        log.info("PPG Vitals Consumer stopped.")


if __name__ == "__main__":
    run()
