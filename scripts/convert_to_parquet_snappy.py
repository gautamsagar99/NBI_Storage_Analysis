from pathlib import Path
import pandas as pd

INPUT_ROOT = Path("../nbi_data_cleaner/data/raw")
OUTPUT_ROOT = Path("data/storage/parquet_snappy")


def main():
    for csv_path in INPUT_ROOT.rglob("*.csv"):
        rel_path = csv_path.relative_to(INPUT_ROOT)
        # Replace .csv with .parquet
        out_rel = rel_path.with_suffix(".parquet")
        out_path = OUTPUT_ROOT / out_rel

        print(f"Converting {csv_path} -> {out_path}")

        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Read with consistent inference
        df = pd.read_csv(csv_path, low_memory=False)

        # Write Parquet with Snappy compression via pyarrow
        df.to_parquet(
            out_path,
            engine="pyarrow",
            compression="snappy",
            index=False,  # usually you don't need the index in files
        )


if __name__ == "__main__":
    main()