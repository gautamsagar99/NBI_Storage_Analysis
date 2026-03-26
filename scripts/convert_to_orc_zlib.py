from pathlib import Path
import pandas as pd
import pyarrow as pa
from pyarrow import orc

INPUT_ROOT = Path("../nbi_data_cleaner/data/raw")
OUTPUT_ROOT = Path("data/storage/orc_zlib")


def clean_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize data types for Arrow compatibility"""
    df = df.copy()
    
    # Fix mixed-type columns
    if "OPR_RATING_METH_063" in df.columns:
        df["OPR_RATING_METH_063"] = pd.to_numeric(
            df["OPR_RATING_METH_063"], errors="coerce"
        )
    
    # Add other type conversions as needed
    return df


def main():
    """Main conversion function"""
    for csv_path in INPUT_ROOT.rglob("*.csv"):
        rel_path = csv_path.relative_to(INPUT_ROOT)
        out_rel = rel_path.with_suffix(".orc")
        out_path = OUTPUT_ROOT / out_rel

        print(f"Converting {csv_path} -> {out_path}")

        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Read with consistent inference
        df = pd.read_csv(csv_path, low_memory=False)

        # Clean/normalize dtypes for Arrow
        df = clean_for_arrow(df)

        table = pa.Table.from_pandas(df)

        # Use zlib compression
        orc.write_table(table, out_path.as_posix(), compression="zlib")


if __name__ == "__main__":
    main()