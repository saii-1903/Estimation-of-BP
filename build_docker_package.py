"""
build_docker_package.py — Package Docker deployment files into a zip.
Run with: python build_docker_package.py
Output: LifeSigns_Vitals_Docker.zip in Documents/
"""
import os
import zipfile
from pathlib import Path

HERE       = Path(__file__).resolve().parent
DOCS       = Path.home() / "Documents"
OUTPUT_ZIP = DOCS / "LifeSigns_Vitals_Docker.zip"

FILES = [
    "Dockerfile",
    "docker-compose.yml",
    ".env.template",
    "requirements.txt",
    "README.md",
    "kafka_config.py",
    "vitals_kafka_consumer.py",
    "vitals_processor.py",
    "vitals_mongo_writer.py",
    "vitals_standalone.py",
    "inference_engine.py",
    "config.py",
]

FOLDERS = [
    "modelsss",
]

def main():
    print(f"[ZIP] Creating {OUTPUT_ZIP} ...")

    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for fname in FILES:
            src = HERE / fname
            if src.exists():
                zf.write(src, f"LifeSigns_Vitals_Docker/{fname}")
                print(f"  + {fname}")
            else:
                print(f"  [SKIP] {fname} not found")

        for folder in FOLDERS:
            src = HERE / folder
            if not src.exists():
                print(f"  [SKIP] {folder}/ not found")
                continue
            for fpath in sorted(src.rglob("*")):
                if fpath.is_file() and "__pycache__" not in str(fpath):
                    arc = f"LifeSigns_Vitals_Docker/{fpath.relative_to(HERE)}"
                    zf.write(fpath, arc)
            print(f"  + {folder}/")

    size_mb = OUTPUT_ZIP.stat().st_size / (1024 * 1024)
    print(f"\n[DONE] {OUTPUT_ZIP}  ({size_mb:.1f} MB)")
    print("\nGive the other team:")
    print("  1. Fill .env.template with Kafka/Mongo details, rename to .env")
    print("  2. docker build -t lifesigns-vitals:latest .")
    print("  3. docker-compose up -d")

if __name__ == "__main__":
    main()
