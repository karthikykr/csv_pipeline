import pyarrow as pa

from engine.runner import PipelineRunner

schema = pa.schema(
    [
        pa.field("col_0", pa.string()),
        pa.field("col_1", pa.string()),
        pa.field("col_2", pa.string()),
    ]
)

runner = PipelineRunner(
    csv_path="data/raw/data.csv",
    parquet_output_dir="data/processed/parquet",
    parquet_schema=schema,
    checkpoint_path="checkpoint/ingestion.chk",
    batch_rows=200_000,
)

runner.run()
