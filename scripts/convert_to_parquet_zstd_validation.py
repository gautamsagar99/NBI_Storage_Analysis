from pathlib import Path
import pandas as pd
import numpy as np

RAW_ROOT = Path("../nbi_data_cleaner/data/raw")
PARQUET_ROOT = Path("data/storage/parquet_zstd")


def list_relative_files(root, suffix):
    return sorted([
        p.relative_to(root)
        for p in root.rglob(f"*{suffix}")
        if p.is_file()
    ])


def dir_size(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def get_all_pairs():
    raw_paths = [p for p in RAW_ROOT.rglob("*.csv")]

    pairs = []
    for raw in raw_paths:
        rel = raw.relative_to(RAW_ROOT)
        pq_rel = rel.with_suffix(".parquet")
        pq = PARQUET_ROOT / pq_rel
        if pq.exists():
            pairs.append((raw, pq))
        else:
            print(f"WARNING: missing Parquet for {raw} -> expected {pq}")
    return pairs


def compare_pair(raw_path: Path, pq_path: Path) -> bool:
    # Load raw CSV and Parquet with consistent settings
    df_raw = pd.read_csv(raw_path, low_memory=False)
    df_pq = pd.read_parquet(pq_path, engine="pyarrow")

    ok = True

    # 1) Shape
    if df_raw.shape != df_pq.shape:
        print(f"SHAPE MISMATCH: {raw_path} vs {pq_path}, "
              f"{df_raw.shape} != {df_pq.shape}")
        ok = False

    # 2) Column names
    if list(df_raw.columns) != list(df_pq.columns):
        print(f"COLUMN MISMATCH: {raw_path} vs {pq_path}")
        print("RAW cols:", df_raw.columns.tolist()[:10], "...")
        print("PARQ cols:", df_pq.columns.tolist()[:10], "...")
        ok = False

    # 3) Basic numeric aggregates on a few columns (adjust as needed)
    numeric_cols = ["ADT", "YEAR_BUILT", "ADT_YEAR"]
    for col in numeric_cols:
        if col in df_raw.columns and col in df_pq.columns:
            r_sum = pd.to_numeric(df_raw[col], errors="coerce").sum()
            p_sum = pd.to_numeric(df_pq[col], errors="coerce").sum()
            if not pd.isna(r_sum) and not pd.isna(p_sum):
                if not np.isclose(r_sum, p_sum, rtol=1e-6, atol=1e-6):
                    print(
                        f"AGG MISMATCH for {col} in {raw_path} vs {pq_path}: "
                        f"sum_raw={r_sum}, sum_parquet={p_sum}"
                    )
                    ok = False

    return ok


def main():
    # 1) Count and missing Parquet files
    raw_files = list_relative_files(RAW_ROOT, ".csv")
    pq_files = list_relative_files(PARQUET_ROOT, ".parquet")

    print("Raw CSV count:", len(raw_files))
    print("Parquet(ZSTD) file count:", len(pq_files))

    missing = []
    for rel in raw_files:
        if rel.with_suffix(".parquet") not in pq_files:
            missing.append(rel)

    print("Missing Parquet(ZSTD) files:", len(missing))
    for m in missing[:10]:
        print("  ", m)

    # 2) Total sizes
    print("Total raw size (MB):", dir_size(RAW_ROOT) / 1024 / 1024, "MB")
    print("Total Parquet(ZSTD) size (MB):", dir_size(PARQUET_ROOT) / 1024 / 1024, "MB")

    # 3) Compare ALL pairs
    pairs = get_all_pairs()
    print("Total comparable pairs:", len(pairs))

    all_ok = True
    for i, (raw, pq) in enumerate(pairs, start=1):
        ok = compare_pair(raw, pq)
        if ok:
            print(f"[{i}/{len(pairs)}] OK: {raw} <-> {pq}")
        else:
            all_ok = False
            # Optional: break early on first failure
            # break

    if all_ok:
        print("All Parquet(ZSTD) files validated successfully.")
    else:
        print("Some Parquet(ZSTD) files had validation issues, investigate further.")


if __name__ == "__main__":
    main()