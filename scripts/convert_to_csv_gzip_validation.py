from pathlib import Path
import pandas as pd

RAW_ROOT = Path("../nbi_data_cleaner/data/raw")
GZ_ROOT = Path("data/storage/csv_gzip")


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
        gz_rel = rel.with_suffix(rel.suffix + ".gz")
        gz = GZ_ROOT / gz_rel
        if gz.exists():
            pairs.append((raw, gz))
        else:
            print(f"WARNING: missing gz for {raw} -> expected {gz}")
    return pairs


def compare_pair(raw_path: Path, gz_path: Path):
    df_raw = pd.read_csv(raw_path, low_memory=False)
    df_gz  = pd.read_csv(gz_path, compression="gzip", low_memory=False)

    # Basic checks
    if df_raw.shape != df_gz.shape:
        print(f"SHAPE MISMATCH: {raw_path} vs {gz_path}, "
              f"{df_raw.shape} != {df_gz.shape}")
        return False

    if list(df_raw.columns) != list(df_gz.columns):
        print(f"COLUMN MISMATCH: {raw_path} vs {gz_path}")
        return False

    # Strict cell‑by‑cell check
    equal = df_raw.equals(df_gz)
    if not equal:
        diff = (df_raw != df_gz)
        print(f"VALUE MISMATCH in {raw_path} vs {gz_path}")
        print(diff.any(axis=1).value_counts())
    return equal


def main():
    # 1) Count and missing gzip files
    raw_files = list_relative_files(RAW_ROOT, ".csv")
    gz_files  = list_relative_files(GZ_ROOT, ".csv.gz")

    print("Raw CSV count:", len(raw_files))
    print("Gzip CSV count:", len(gz_files))

    missing = []
    for rel in raw_files:
        if rel.with_suffix(rel.suffix + ".gz") not in gz_files:
            missing.append(rel)

    print("Missing gz files:", len(missing))
    for m in missing[:10]:
        print("  ", m)

    # 2) Total sizes
    print("Total raw size (MB):", dir_size(RAW_ROOT) / 1024 / 1024, "MB")
    print("Total gzip size (MB):", dir_size(GZ_ROOT) / 1024 / 1024, "MB")

    # 3) Compare ALL pairs
    pairs = get_all_pairs()
    print("Total comparable pairs:", len(pairs))

    all_ok = True
    for i, (raw, gz) in enumerate(pairs, start=1):
        ok = compare_pair(raw, gz)
        if ok:
            print(f"[{i}/{len(pairs)}] OK: {raw} <-> {gz}")
        else:
            all_ok = False
            # Optional: break early on first failure
            # break

    if all_ok:
        print("All files match perfectly.")
    else:
        print("Some files had mismatches, investigate further.")


if __name__ == "__main__":
    main()