"""
vitals_test_producer.py — Send PPG vitals JSON files to Kafka for testing.

Usage:
    python vitals_test_producer.py                          # sends all files in data/training_data_200hz/
    python vitals_test_producer.py data/ppgbp_converted/   # different folder
    python vitals_test_producer.py --delay 2               # 2s between messages (default 0.5)
    python vitals_test_producer.py --limit 5               # send only first 5 files
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

import kafka_config as cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def _delivery_report(err, msg):
    if err:
        log.error(f"Delivery failed | {err}")
    else:
        log.info(f"Delivered → topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def run(folders: list[str], delay: float, limit: int | None):
    producer = Producer({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP_SERVERS})

    files: list[Path] = []
    for folder in folders:
        p = Path(folder)
        if not p.exists():
            log.warning(f"Folder not found: {p} — skipping")
            continue
        files.extend(sorted(p.glob("*.json")))

    if not files:
        log.error("No JSON files found in specified folders.")
        sys.exit(1)

    if limit:
        files = files[:limit]

    log.info(
        f"Sending {len(files)} file(s) → topic={cfg.KAFKA_TOPIC} "
        f"broker={cfg.KAFKA_BOOTSTRAP_SERVERS} delay={delay}s"
    )

    for i, fpath in enumerate(files, 1):
        try:
            payload = json.loads(fpath.read_bytes())
        except Exception as exc:
            log.warning(f"[{i}/{len(files)}] Skip {fpath.name}: {exc}")
            continue

        # Ensure required fields are present (add defaults if missing)
        payload.setdefault("admissionId", f"TEST-{fpath.stem}")
        payload.setdefault("patientId",   "TEST-PATIENT")
        payload.setdefault("facilityId",  cfg.FACILITY_ID)
        payload.setdefault("deviceId",    "TEST-DEVICE")

        key = payload.get("admissionId", fpath.stem).encode()
        value = json.dumps(payload).encode()

        producer.produce(
            topic=cfg.KAFKA_TOPIC,
            key=key,
            value=value,
            callback=_delivery_report,
        )
        producer.poll(0)  # trigger callbacks

        log.info(f"[{i}/{len(files)}] Queued {fpath.name} ({len(value):,} bytes)")

        if delay > 0 and i < len(files):
            time.sleep(delay)

    log.info("Flushing producer...")
    producer.flush()
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send PPG vitals JSON files to Kafka")
    parser.add_argument(
        "folders",
        nargs="*",
        default=["data/training_data_200hz"],
        help="Folder(s) containing .json files (default: data/training_data_200hz)",
    )
    parser.add_argument("--delay",  type=float, default=0.5, help="Seconds between messages (default 0.5)")
    parser.add_argument("--limit",  type=int,   default=None, help="Max number of files to send")
    args = parser.parse_args()

    run(args.folders, args.delay, args.limit)
