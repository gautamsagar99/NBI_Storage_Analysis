import os
from pathlib import Path
import pandas as pd

INPUT_ROOT = Path("../nbi_data_cleaner/data/raw")
OUTPUT_ROOT = Path("data/storage/csv_gzip")

def main():
    for csv_path in INPUT_ROOT.rglob("*.csv"):
        rel_path = csv_path.relative_to(INPUT_ROOT)
        # Change extension: .csv -> .csv.gz
        out_rel = rel_path.with_suffix(rel_path.suffix + ".gz")
        out_path = OUTPUT_ROOT / out_rel

        print(f"Converting {csv_path} -> {out_path}")

        out_path.parent.mkdir(parents=True, exist_ok=True)

        df = pd.read_csv(csv_path, low_memory=False)
        # Write compressed CSV (gzip)
        df.to_csv(out_path, index=False, compression="gzip")

if __name__ == "__main__":
    main()
