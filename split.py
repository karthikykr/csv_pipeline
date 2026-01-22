import csv
import os
import time
from typing import List

SOURCE_DIR = "source"
SPLIT_DIR = "splitted"
ROWS_PER_FILE = 100_000


def count_rows(csv_path: str) -> int:
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        return sum(1 for _ in reader)


def split_csv(
    csv_path: str,
    source_name: str,
    output_dir: str,
    rows_per_file: int,
) -> List[int]:
    os.makedirs(output_dir, exist_ok=True)

    rows_per_split: List[int] = []

    with open(csv_path, "r", newline="", encoding="utf-8") as source_file:
        reader = csv.reader(source_file)
        header = next(reader)

        file_index = 1
        current_row_count = 0
        output_file = None
        writer = None

        for row in reader:
            if current_row_count == 0:
                output_path = os.path.join(
                    output_dir,
                    f"{source_name}_split_{file_index}.csv",
                )
                output_file = open(output_path, "w", newline="", encoding="utf-8")
                writer = csv.writer(output_file)
                writer.writerow(header)
                rows_per_split.append(0)

            writer.writerow(row)
            current_row_count += 1
            rows_per_split[-1] += 1

            if current_row_count >= rows_per_file:
                output_file.close()
                file_index += 1
                current_row_count = 0

        if output_file and not output_file.closed:
            output_file.close()

    return rows_per_split


if __name__ == "__main__":
    start_time = time.time()

    source_files = [f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")]

    if not source_files:
        raise FileNotFoundError("No CSV files found in source folder")

    grand_total_source_rows = 0
    grand_total_split_rows = 0
    total_split_files = 0

    for csv_file in source_files:
        source_name = os.path.splitext(csv_file)[0]
        csv_path = os.path.join(SOURCE_DIR, csv_file)

        print(f"\nProcessing source file: {csv_file}")

        source_rows = count_rows(csv_path)
        print(f"Source rows: {source_rows:,}")
        grand_total_source_rows += source_rows

        print("Splitting CSV...")
        split_counts = split_csv(
            csv_path,
            source_name,
            SPLIT_DIR,
            ROWS_PER_FILE,
        )

        for i, count in enumerate(split_counts, start=1):
            print(f"{source_name}_split_{i}.csv -> {count:,} rows")

        total_rows = sum(split_counts)
        total_split_files += len(split_counts)
        grand_total_split_rows += total_rows

        if total_rows == source_rows:
            print("Row counts match for this file")
        else:
            print("Row mismatch detected for this file")

    print("\nFinal Summary")
    print(f"Total source rows: {grand_total_source_rows:,}")
    print(f"Total split files: {total_split_files}")
    print(f"Total rows in split files: {grand_total_split_rows:,}")

    if grand_total_source_rows == grand_total_split_rows:
        print("All rows processed correctly")
    else:
        print("Data loss detected")

    end_time = time.time()
    elapsed = end_time - start_time
    print(f"\nTotal execution time: {elapsed:.2f} seconds")
