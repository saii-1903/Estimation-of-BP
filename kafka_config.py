"""
kafka_config.py — All configuration from environment variables (12-factor app).
Vitals estimation pipeline (PPG → BP / Hb / Glucose).
"""
import os

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",             "ppg-vitals-topic")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID",          "ppg-vitals-consumer-group")

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
MONGO_URI        = os.getenv("MONGO_URI",        "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB",         "vitals_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "ppg_vitals_results")

# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------
SAMPLING_RATE_HZ    = 120          # target rate (all devices processed at 120 Hz)
MIN_SAMPLES         = SAMPLING_RATE_HZ * 25   # 3000 — minimum acceptable signal

# Reference vs AI mismatch — triggers "take another manual reading" alert on first LS06.
# Also used for post-calibration drift detection.
BP_MISMATCH_THRESHOLD_SBP = int(os.getenv("BP_MISMATCH_THRESHOLD_SBP", "10"))
BP_MISMATCH_THRESHOLD_DBP = int(os.getenv("BP_MISMATCH_THRESHOLD_DBP", "10"))

# Session-to-session trend alert — fires in all modes (with or without reference).
BP_TREND_THRESHOLD_SBP = int(os.getenv("BP_TREND_THRESHOLD_SBP", "15"))
BP_TREND_THRESHOLD_DBP = int(os.getenv("BP_TREND_THRESHOLD_DBP", "15"))

# Two LS06 readings must agree within this to confirm the reference.
LS06_CONFIRM_TOLERANCE = int(os.getenv("LS06_CONFIRM_TOLERANCE", "5"))

# ---------------------------------------------------------------------------
# Deployment
# ---------------------------------------------------------------------------
FACILITY_ID = os.getenv("FACILITY_ID", "CF0000000000")
LOG_LEVEL   = os.getenv("LOG_LEVEL",   "INFO")
