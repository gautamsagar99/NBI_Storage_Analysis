from pathlib import Path
import pandas as pd

INPUT_ROOT = Path("../nbi_data_cleaner/data/raw")
OUTPUT_ROOT = Path("data/storage/parquet_zstd")


def main():
    for csv_path in INPUT_ROOT.rglob("*.csv"):
        rel_path = csv_path.relative_to(INPUT_ROOT)
        out_rel = rel_path.with_suffix(".parquet")
        out_path = OUTPUT_ROOT / out_rel

        print(f"Converting {csv_path} -> {out_path}")

        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Read with consistent inference
        df = pd.read_csv(csv_path, low_memory=False)

        # Write Parquet with ZSTD compression via pyarrow
        df.to_parquet(
            out_path,
            engine="pyarrow",
            compression="zstd",
            index=False,
        )


if __name__ == "__main__":
    main()