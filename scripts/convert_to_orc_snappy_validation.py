from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.orc as orc

RAW_ROOT = Path("../nbi_data_cleaner/data/raw")
ORC_ROOT = Path("data/storage/orc_snappy")


def clean_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "OPR_RATING_METH_063" in df.columns:
        df["OPR_RATING_METH_063"] = pd.to_numeric(
            df["OPR_RATING_METH_063"], errors="coerce"
        )
    return df


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
        orc_rel = rel.with_suffix(".orc")
        orc_path = ORC_ROOT / orc_rel
        if orc_path.exists():
            pairs.append((raw, orc_path))
        else:
            print(f"WARNING: missing ORC for {raw} -> expected {orc_path}")
    return pairs


def compare_pair(raw_path: Path, orc_path: Path) -> bool:
    # Load raw CSV with same logic as conversion
    df_raw = pd.read_csv(raw_path, low_memory=False)
    df_raw = clean_for_arrow(df_raw)

    # Load ORC via pyarrow, then to pandas (NO context manager)
    reader = orc.ORCFile(orc_path.as_posix())
    table = reader.read()
    df_orc = table.to_pandas()

    ok = True

    # 1) Shape
    if df_raw.shape != df_orc.shape:
        print(f"SHAPE MISMATCH: {raw_path} vs {orc_path}, "
              f"{df_raw.shape} != {df_orc.shape}")
        ok = False

    # 2) Column names
    if list(df_raw.columns) != list(df_orc.columns):
        print(f"COLUMN MISMATCH: {raw_path} vs {orc_path}")
        print("RAW cols:", df_raw.columns.tolist()[:10], "...")
        print("ORC cols:", df_orc.columns.tolist()[:10], "...")
        ok = False

    # 3) Basic numeric aggregates on a few columns (adjust names)
    numeric_cols = [
        "ADT", "YEAR_BUILT", "ADT_YEAR", "OPR_RATING_METH_063"
    ]
    for col in numeric_cols:
        if col in df_raw.columns and col in df_orc.columns:
            r_sum = pd.to_numeric(df_raw[col], errors="coerce").sum()
            o_sum = pd.to_numeric(df_orc[col], errors="coerce").sum()
            if not pd.isna(r_sum) and not pd.isna(o_sum):
                if not np.isclose(r_sum, o_sum, rtol=1e-6, atol=1e-6):
                    print(
                        f"AGG MISMATCH for {col} in {raw_path} vs {orc_path}: "
                        f"sum_raw={r_sum}, sum_orc={o_sum}"
                    )
                    ok = False

    return ok


def main():
    # 1) Count and missing ORC files
    raw_files = list_relative_files(RAW_ROOT, ".csv")
    orc_files = list_relative_files(ORC_ROOT, ".orc")

    print("Raw CSV count:", len(raw_files))
    print("ORC file count:", len(orc_files))

    missing = []
    for rel in raw_files:
        if rel.with_suffix(".orc") not in orc_files:
            missing.append(rel)

    print("Missing ORC files:", len(missing))
    for m in missing[:10]:
        print("  ", m)

    # 2) Total sizes
    print("Total raw size (MB):", dir_size(RAW_ROOT) / 1024 / 1024, "MB")
    print("Total ORC size (MB):", dir_size(ORC_ROOT) / 1024 / 1024, "MB")

    # 3) Compare ALL pairs
    pairs = get_all_pairs()
    print("Total comparable pairs:", len(pairs))

    all_ok = True
    for i, (raw, orc_path) in enumerate(pairs, start=1):
        ok = compare_pair(raw, orc_path)
        if ok:
            print(f"[{i}/{len(pairs)}] OK: {raw} <-> {orc_path}")
        else:
            all_ok = False
            # Optional: break early
            # break

    if all_ok:
        print("All ORC files validated successfully.")
    else:
        print("Some ORC files had validation issues, investigate further.")


if __name__ == "__main__":
    main()