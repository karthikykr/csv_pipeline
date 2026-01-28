import os
import re

import polars as pl


class DataCleaningNormalizer:
    @staticmethod
    def normalize_party_expression(expr: pl.Expr) -> pl.Expr:
        return (
            expr.str.replace_all(
                r"(?i)(\bd\s*[/.\-]\s*b\s*[/.\-]\s*a\b|\bdba\b|doing\s+business\s+as).*",
                "",
            )
            .str.replace_all(r"(?i)[,\s]*\bN\s?\.?\s?A\s?\.?\b", "")
            .str.replace_all(r"(?i)\bGRAND\s+JURY\s+MATTER\s+1\b", "")
            .str.replace_all(r"\([^)]*\)|\[[^]]*\]|\{[^}]*\}", "")
            .str.replace_all(r"\s+|\s+$", " ")
            .str.replace_all(
                r"(?i)[,\s]*(\bmodel\s*number|\bphone\s*number|\btracking\s*number|"
                r"\bsn\b\s*:?\s*[A-Za-z0-9]+|s\s*/s*n|etc|f\s*/\s*k\s*/\s*a|"
                r"imei|seizure\s+(?:No\.?|number)|delivery\s+confirmation|"
                r"number\s+|label\s+no\.?|located\s+at|"
                r"d\s*[\/-]\s*o\s*[\/-]\s*b|identification\s+number|"
                r"tag\s+number|\btag\b|express\s+service\s+number).*",
                "",
            )
            .str.replace_all(r"^[^A-Za-z0-9]+", "")
            .str.replace_all(
                r"(?i)(\bMail\s+Parcel\b|\bMail\s+express\s*parcel\b|\bexpress\s+mail\b).*",
                "",
            )
            .str.replace_all(
                r"(?i)\b((vehicle|serial)\s*(number|no\.?)?\s*:?)\b\s*[A-Za-z0-9\-_.]+.*",
                "",
            )
            .str.replace_all(
                r"(?i)[,\s]*(<b>|ip\s*address|imsi|formerly\s*known\s+as|"
                r"now\s+known\s+as\s+|ca\s*\d+[/-]\d+\b|"
                r"\ba\s*?k\s*?a\s*?\b|\bas\s+chapter\s+\d+\b|"
                r"\bIn\s+his\s+capacity\s+as\b).*",
                "",
            )
            .str.replace_all(r"(?i)[,\s]*\bsealed\b.*", "")
            .str.replace_all(r"(?i)[,\s]*[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}.*", "")
            .str.replace_all(r"\([^)]*$", "")
            .str.replace_all(
                r"(?i)[,\s]*("
                r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|"
                r"\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b|"
                r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
                r"[a-z]*\.?\s+\d{1,2}\s+\d{2,4}\b"
                r").*",
                "",
            )
            .str.replace_all(r"(?i)\b(LLC|LTD|INC|CORP|CO\.?|COMPANY)\b.*", r"$1")
            .str.replace_all(r"(?i)\bBANK\b.*", "Bank")
            .str.replace_all(r"(?i)\b(phone\s*(no\.?|number))\b\s*\d+.*", "")
            .str.replace_all(
                r"(?i)\b(model\s*(no\.?|number))\b\s*[A-Za-z0-9\-_.]+.*",
                "",
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
    BASE_PATH = "/pipeline"
    INPUT_DIR = f"{BASE_PATH}/splitted"
    OUTPUT_DIR = f"{BASE_PATH}/clean"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cleaned = {
        f.replace("clean_", "")
        for f in os.listdir(OUTPUT_DIR)
        if f.startswith("clean_")
    }

    split_files = sorted(
        f for f in os.listdir(INPUT_DIR) if f.endswith(".csv") and f not in cleaned
    )

    for filename in split_files:
        inp = os.path.join(INPUT_DIR, filename)
        out = os.path.join(OUTPUT_DIR, f"clean_{filename}")

        (
            pl.scan_csv(inp, ignore_errors=True)
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
            .sink_csv(out)
        )


if __name__ == "__main__":
    main()
