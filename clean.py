import os
import re
import shutil
from pathlib import Path

import polars as pl

BASE_PATH = "/pipeline"

SOURCE_DIR = Path(f"{BASE_PATH}/source")
SPLIT_DIR = Path(f"{BASE_PATH}/splitted")
CLEAN_DIR = Path(f"{BASE_PATH}/clean")
PROCESSED_DIR = Path(f"{BASE_PATH}/processed")

CLEAN_DIR.mkdir(exist_ok=True)
PROCESSED_DIR.mkdir(exist_ok=True)


class DataCleaningNormalizer:
    @staticmethod
    def normalize_party_expression(expr: pl.Expr) -> pl.Expr:
        return (
            expr
            # 1. Remove anything after d/b/a or doing business as
            .str.replace_all(
                r"(?i)(\bd\s*[/.\-]\s*b\s*[/.\-]\s*a\b|\bdba\b|doing\s+business\s+as).*",
                "",
            )
            # 2. Remove N.A / N . A
            .str.replace_all(r"(?i)[,\s]*\bN\s?\.?\s?A\s?\.?\b", "")
            # 3. Remove GRAND JURY MATTER 1
            .str.replace_all(r"(?i)\bGRAND\s+JURY\s+MATTER\s+1\b", "")
            # 4. Remove balanced (), [], {}
            .str.replace_all(r"\([^)]*\)|\[[^]]*\]|\{[^}]*\}", "")
            # 5. Normalize whitespace
            .str.replace_all(r"\s+|\s+$", " ")
            # 6. Removes identifiers, tracking info, and related trailing content
            .str.replace_all(
                r"(?i)[,\s]*(\bmodel\s*number|\bphone\s*number|\btracking\s*number|"
                r"\bsn\b\s*:?\s*[A-Za-z0-9]+|s\s*/s*n|etc|f\s*/\s*k\s*/\s*a|"
                r"imei|seizure\s+(?:No\.?|number)|delivery\s+confirmation|"
                r"number\s+|label\s+no\.?|located\s+at|"
                r"d\s*[\/-]\s*o\s*[\/-]\s*b|identification\s+number|"
                r"tag\s+number|\btag\b|express\s+service\s+number).*",
                "",
            )
            # 7. Removes leading special characters
            .str.replace_all(r"^[^A-Za-z0-9]+", "")
            # 8. Removes anything after mail parcel variants
            .str.replace_all(
                r"(?i)(\bMail\s+Parcel\b|\bMail\s+express\s*parcel\b|\bexpress\s+mail\b).*",
                "",
            )
            # 9. Removes serial / vehicle numbers
            .str.replace_all(
                r"(?i)\b((vehicle|serial)\s*(number|no\.?)?\s*:?)\b\s*"
                r"[a-zA-Z0-9\-_.]+.*",
                "",
            )
            # 10. Removes in his capacity, AKA, chapter references, ip address, imsi, formerly known as, now known as, CA 2/9, as chapter,'''
            .str.replace_all(
                r"(?i)[,\s]*(<b>|ip\s*address|imsi|formerly\s*known\s*as|"
                r"now\s+known\s+as\s+|ca\s*\d+[/-]\d+\b|"
                r"\ba\s*?k\s*?a\s*?\b|\bas\s+chapter\s+\d+\b|"
                r"\bIn\s+his\s+capacity\s+as\b).*",
                "",
            )
            # 11. Remove sealed cases
            .str.replace_all(r"(?i)[,\s]*\bsealed\b.*", "")
            # 12. Remove email and everything after
            .str.replace_all(r"(?i)[,\s]*[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}.*", "")
            # 13. Remove unclosed opening parenthesis and everything after
            .str.replace_all(r"\([^)]*$", "")
            # 14. Remove dates and everything after (multiple formats)
            .str.replace_all(
                r"(?i)[,\s]*("
                r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|"
                r"\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b|"
                r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
                r"[a-z]*\.?\s+\d{1,2}\s+\d{2,4}\b"
                r").*",
                "",
            )
            # 15. Truncate after corporate suffix
            .str.replace_all(r"(?i)\b(LLC|LTD|INC|CORP|CO\.?|COMPANY)\b.*", r"$1")
            # 16. Keep BANK, remove anything after
            .str.replace_all(r"(?i)\bBANK\b.*", "Bank")
            # 17. Remove phone numbers
            .str.replace_all(r"(?i)\b(phone\s*(no\.?|number))\b\s*\d+.*", "")
            # 18. Remove model numbers
            .str.replace_all(
                r"(?i)\b(model\s*(no\.?|number))\b\s*[a-zA-Z0-9\-_.]+.*", ""
            )
        )


V_REGEX = re.compile(r"\s+v\.?\s+|\s+vs\.?\s+|\s+versus\s+", re.IGNORECASE)
ET_AL_CLEANER = re.compile(r",?\s+ET\s+AL\.?$", re.IGNORECASE)


def clean_party(name: str | None) -> str | None:
    if not isinstance(name, str):
        return None
    return ET_AL_CLEANER.sub("", name.strip()).strip()


def parse_case_name(case_name: str | None) -> dict:
    if not isinstance(case_name, str):
        return {"plaintiff": None, "defendant": None}

    parts = V_REGEX.split(case_name, maxsplit=1)
    if len(parts) != 2:
        return {"plaintiff": None, "defendant": None}

    return {
        "plaintiff": clean_party(parts[0]),
        "defendant": clean_party(parts[1]),
    }


def main():
    cleaned_files = {
        f.replace("clean_", "") for f in os.listdir(CLEAN_DIR) if f.startswith("clean_")
    }

    split_files = sorted(
        f
        for f in os.listdir(SPLIT_DIR)
        if f.endswith(".csv") and f not in cleaned_files
    )

    if not split_files:
        print("No new split files to clean.")
        return

    for filename in split_files:
        input_path = SPLIT_DIR / filename
        output_path = CLEAN_DIR / f"clean_{filename}"

        (
            pl.scan_csv(input_path, ignore_errors=True)
            .with_columns(
                pl.col("case_name")
                .map_elements(
                    parse_case_name,
                    return_dtype=pl.Struct(
                        [
                            pl.Field("plaintiff", pl.Utf8),
                            pl.Field("defendant", pl.Utf8),
                        ]
                    ),
                )
                .alias("parsed")
            )
            .with_columns(
                pl.col("parsed").struct.field("plaintiff").alias("plaintiff"),
                pl.col("parsed").struct.field("defendant").alias("defendant"),
                DataCleaningNormalizer.normalize_party_expression(
                    pl.col("parsed").struct.field("plaintiff")
                ).alias("plaintiff_norm"),
                DataCleaningNormalizer.normalize_party_expression(
                    pl.col("parsed").struct.field("defendant")
                ).alias("defendant_norm"),
            )
            .select(
                [
                    "case_name",
                    "plaintiff",
                    "defendant",
                    "plaintiff_norm",
                    "defendant_norm",
                ]
            )
            .sink_csv(output_path)
        )

        print(f"Cleaned: {filename}")

    for source_file in SOURCE_DIR.glob("*.csv"):
        target = PROCESSED_DIR / source_file.name
        shutil.move(str(source_file), str(target))
        print(f"Archived source file: {source_file.name}")


if __name__ == "__main__":
    main()
