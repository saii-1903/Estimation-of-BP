"""
vitals_mongo_writer.py — Write PPG vitals results to MongoDB.
Same pattern as arrhythmia mongo_writer.py.
"""
from __future__ import annotations

import logging
import time

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

import kafka_config as cfg

log = logging.getLogger(__name__)

_client: MongoClient | None = None


def get_collection():
    global _client
    if _client is None:
        _client = MongoClient(cfg.MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client[cfg.MONGO_DB][cfg.MONGO_COLLECTION]


def write_result(doc: dict, retries: int = 3) -> str:
    """
    Write vitals result document to MongoDB.
    Returns the document UUID on success.
    Raises RuntimeError after retries are exhausted.
    """
    for attempt in range(1, retries + 1):
        try:
            col = get_collection()
            col.insert_one(doc)
            log.info(
                f"{doc.get('admissionId')} | Written to MongoDB "
                f"(uuid={doc.get('uuid')}) collection={cfg.MONGO_COLLECTION}"
            )
            return doc["uuid"]
        except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
            log.warning(f"MongoDB write attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"MongoDB unavailable after {retries} attempts") from exc
        except Exception as exc:
            log.error(f"MongoDB unexpected error: {exc}")
            raise
