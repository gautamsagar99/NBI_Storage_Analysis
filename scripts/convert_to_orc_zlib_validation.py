from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
import numpy as np

# Configuration
RAW_ROOT = Path("../nbi_data_cleaner/data/raw")
ORC_ROOT = Path("data/storage/orc_zlib")


def list_relative_files(root, suffix):
    """List files relative to root directory"""
    return sorted([
        p.relative_to(root)
        for p in root.rglob(f"*{suffix}")
        if p.is_file()
    ])


def dir_size(root: Path) -> int:
    """Calculate total directory size"""
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def get_all_pairs():
    """Get all CSV/ORC file pairs"""
    raw_paths = [p for p in RAW_ROOT.rglob("*.csv")]
    pairs = []
    
    for raw in raw_paths:
        rel = raw.relative_to(RAW_ROOT)
        orc_rel = rel.with_suffix(".orc")
        orc_path = ORC_ROOT / orc_rel
        
        if orc_path.exists():
            pairs.append((raw, orc_path))
        else:
            print(f"WARNING: Missing ORC file for {raw} -> expected {orc_path}")
            
    return pairs


def compare_pair(raw_path: Path, orc_path: Path) -> bool:
    """Compare CSV and ORC file contents"""
    try:
        # Load files
        df_raw = pd.read_csv(raw_path, low_memory=False)
        df_orc = orc.read_table(orc_path).to_pandas()
        
        ok = True
        
        # 1) Shape comparison
        if df_raw.shape != df_orc.shape:
            print(f"SHAPE MISMATCH: {raw_path} vs {orc_path}, "
                  f"{df_raw.shape} != {df_orc.shape}")
            ok = False
            
        # 2) Column names comparison
        if list(df_raw.columns) != list(df_orc.columns):
            print(f"COLUMN MISMATCH: {raw_path} vs {orc_path}")
            print("  RAW cols:", df_raw.columns.tolist()[:10], "...")
            print("  ORC cols:", df_orc.columns.tolist()[:10], "...")
            ok = False
            
        # 3) Numeric aggregates comparison
        numeric_cols = ["ADT", "YEAR_BUILT", "ADT_YEAR"]
        for col in numeric_cols:
            if col in df_raw.columns and col in df_orc.columns:
                r_sum = pd.to_numeric(df_raw[col], errors="coerce").sum()
                o_sum = pd.to_numeric(df_orc[col], errors="coerce").sum()
                
                if not pd.isna(r_sum) and not pd.isna(o_sum):
                    if not np.isclose(r_sum, o_sum, rtol=1e-6, atol=1e-6):
                        print(f"AGG MISMATCH for {col} in {raw_path} vs {orc_path}: "
                              f"sum_raw={r_sum}, sum_orc={o_sum}")
                        ok = False
        
        return ok
        
    except Exception as e:
        print(f"ERROR comparing {raw_path} and {orc_path}: {e}")
        return False


def main():
    """Main validation function"""
    # 1) Count and missing files
    raw_files = list_relative_files(RAW_ROOT, ".csv")
    orc_files = list_relative_files(ORC_ROOT, ".orc")
    
    print(f"Raw CSV count: {len(raw_files)}")
    print(f"ORC file count: {len(orc_files)}")
    
    missing = []
    for rel in raw_files:
        if rel.with_suffix(".orc") not in orc_files:
            missing.append(rel)
            
    print(f"Missing ORC files: {len(missing)}")
    for m in missing[:10]:
        print(f"  {m}")
    
    # 2) Total sizes
    print(f"Total raw size: {dir_size(RAW_ROOT) / 1024 / 1024:.2f} MB")
    print(f"Total ORC size: {dir_size(ORC_ROOT) / 1024 / 1024:.2f} MB")
    
    # 3) Compare all pairs
    pairs = get_all_pairs()
    print(f"Total comparable pairs: {len(pairs)}")
    
    all_ok = True
    for i, (raw, orc_file) in enumerate(pairs, start=1):
        ok = compare_pair(raw, orc_file)
        if ok:
            print(f"[{i}/{len(pairs)}] OK: {raw} <-> {orc_file}")
        else:
            all_ok = False
            
    if all_ok:
        print("All ORC zlib files validated successfully.")
    else:
        print("Some ORC zlib files had validation issues. Please investigate.")
        exit(1)


if __name__ == "__main__":
    main()