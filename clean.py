import polars as pl


class DataCleaningNormalizer:
    def __init__(self, input_csv: str, output_csv: str) -> None:
        """
        :param input_csv: Path to input CSV file
        :param output_csv: Path to output CSV file

        """
        self.input_csv = input_csv
        self.output_csv = output_csv

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

    def run(self) -> None:
        """
        Executes the normalization pipeline:
        - Lazily reads CSV
        - Normalizes plaintiff and defendant columns
        - Writes output CSV without loading full data into memory
        """

        df = pl.scan_csv(self.input_csv, ignore_errors=True)

        df = df.with_columns(
            [
                self.normalize_party_expression(pl.col("plaintiff")).alias(
                    "plaintiff_norm"
                ),
                self.normalize_party_expression(pl.col("defendant")).alias(
                    "defendant_norm"
                ),
            ]
        )

        df.sink_csv(self.output_csv)

        print(f"Normalized data written to {self.output_csv}")


if __name__ == "__main__":
    INPUT_CSV = "parsed_case_names.csv"
    OUTPUT_CSV = "output.csv"

    normalizer = DataCleaningNormalizer(INPUT_CSV, OUTPUT_CSV)
    normalizer.run()
