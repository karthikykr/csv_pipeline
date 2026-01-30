import json
import os
import re

import polars as pl

BASE_PATH = "/pipeline"
INPUT_FILE = f"{BASE_PATH}/source"
OUTPUT_DIR = f"{BASE_PATH}/splitted"

STATE_DIR = "/tmp/csv_pipeline_state"
STATE_FILE = f"{STATE_DIR}/split_checkpoint.json"

ROWS_PER_FILE = 400_000

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)


def load_state() -> int:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)["rows_processed"]
    return 0


def save_state(rows: int) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump({"rows_processed": rows}, f)


def last_split_index() -> int:
    pattern = re.compile(r"split_(\d+)\.csv")
    indices = [
        int(m.group(1)) for f in os.listdir(OUTPUT_DIR) if (m := pattern.match(f))
    ]
    return max(indices) if indices else 0


df = pl.scan_csv(
    INPUT_FILE,
    infer_schema_length=100_000,
    schema_overrides={"pacer_case_id": pl.Utf8},
)

offset = load_state()
file_index = last_split_index() + 1

while True:
    chunk = df.slice(offset, ROWS_PER_FILE).collect()

    if chunk.is_empty():
        break

    out = f"{OUTPUT_DIR}/split_{file_index}.csv"
    chunk.write_csv(out)

    offset += len(chunk)
    save_state(offset)
    file_index += 1
