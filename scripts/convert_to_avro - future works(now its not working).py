from pathlib import Path
import pandas as pd
import pandavro as pdx  # pip install pandavro

INPUT_ROOT = Path("../nbi_data_cleaner/data/raw")
OUTPUT_ROOT = Path("data/storage/avro")


def clean_for_avro(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Example: fix known mixed-type numeric columns (adjust as needed)
    if "OPR_RATING_METH_063" in df.columns:
        df["OPR_RATING_METH_063"] = pd.to_numeric(
            df["OPR_RATING_METH_063"], errors="coerce"
        )

    # You can add more columns here if pandavro/fastavro complains
    return df


def main():
    for csv_path in INPUT_ROOT.rglob("*.csv"):
        rel_path = csv_path.relative_to(INPUT_ROOT)
        out_rel = rel_path.with_suffix(".avro")
        out_path = OUTPUT_ROOT / out_rel

        print(f"Converting {csv_path} -> {out_path}")

        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Read with consistent inference
        df = pd.read_csv(csv_path, low_memory=False)

        # Optional: clean dtypes to avoid Avro schema issues
        df = clean_for_avro(df)

        # Schema is inferred from the DataFrame by pandavro
        pdx.to_avro(out_path.as_posix(), df)


if __name__ == "__main__":
    main()