import os

import polars as pl

INPUT_FILE = "source"
OUTPUT_DIR = "splitted"
ROWS_PER_FILE = 800_000

os.makedirs(OUTPUT_DIR, exist_ok=True)

df = pl.scan_csv(
    INPUT_FILE,
    infer_schema_length=100_000,
    schema_overrides={
        "pacer_case_id": pl.Utf8,
    },
)

offset = 0
file_index = 1

while True:
    chunk = df.slice(offset, ROWS_PER_FILE).collect(engine="streaming")

    if chunk.is_empty():
        break

    output_path = f"{OUTPUT_DIR}/split_{file_index}.csv"
    chunk.write_csv(output_path)

    print(f"Wrote {output_path} -> {len(chunk):,} rows")

    offset += ROWS_PER_FILE
    file_index += 1
