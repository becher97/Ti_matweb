#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Merge new alloy composition data into results-csv.* by matching `source_folder`.

Behavior:
- Reads base Excel `results-csv.xls` (or `results-csv.xlsx` if it exists).
- Reads a provided new Excel file (e.g., `new_result.xlsx`).
- Updates the `合金成分` column in base where it is empty/NaN using values
  from the new file matched by `source_folder`.
- Appends rows that exist in the new file but not in the base, keeping
  only columns that exist in the base (others are ignored).
- Writes the merged result to `results-csv.xlsx` with sheet name `results csv`.

Notes:
- Requires pandas and openpyxl (for writing xlsx). If base is `.xls`,
  xlrd is required to read it.
- Source file encoding is UTF-8. Excel encoding is handled by engines.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import List, Set

import pandas as pd


BASE_XLS = Path("results-csv.xls")
BASE_XLSX = Path("results-csv.xlsx")
SHEET_NAME = "results csv"
KEY_COL = "source_folder"
TARGET_COLS: List[str] = ["合金成分"]


def read_base_df() -> pd.DataFrame:
    # Prefer existing xlsx as base; else fall back to xls
    if BASE_XLSX.exists():
        df = pd.read_excel(BASE_XLSX, sheet_name=0)
    elif BASE_XLS.exists():
        # read .xls via xlrd
        df = pd.read_excel(BASE_XLS, sheet_name=0, engine="xlrd")
    else:
        raise FileNotFoundError("Base file results-csv.xls/.xlsx not found")

    # Ensure key exists
    if KEY_COL not in df.columns:
        raise KeyError(f"Base missing key column: {KEY_COL}")

    return df


def normalize_key(series: pd.Series) -> pd.Series:
    # Keep as string for robust matching
    return series.astype(str).str.strip()


def is_missing(val) -> bool:
    if pd.isna(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def merge_new(base_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if KEY_COL not in new_df.columns:
        raise KeyError(f"New file missing key column: {KEY_COL}")

    # Normalize keys
    base_df = base_df.copy()
    new_df = new_df.copy()
    base_df[KEY_COL] = normalize_key(base_df[KEY_COL])
    new_df[KEY_COL] = normalize_key(new_df[KEY_COL])

    # Use the first occurrence per key in new data
    new_df = new_df.drop_duplicates(subset=[KEY_COL], keep="first")

    # Build a map for target columns from new
    new_map = {col: new_df.set_index(KEY_COL)[col] for col in TARGET_COLS if col in new_df.columns}

    # Update existing rows for target columns
    if new_map:
        base_df.set_index(KEY_COL, inplace=True)
        for col, s in new_map.items():
            # Ensure column exists in base; if not, create it
            if col not in base_df.columns:
                base_df[col] = pd.NA

            # Align to base index
            aligned = s.reindex(base_df.index)

            # Where base is missing and new has value, fill
            mask = base_df[col].apply(is_missing) & ~aligned.apply(is_missing)
            base_df.loc[mask, col] = aligned.loc[mask]

        base_df.reset_index(inplace=True)
    else:
        # No matching target columns present in new
        base_df = base_df.copy()

    # Append new rows that don't exist in base
    base_keys = set(normalize_key(base_df[KEY_COL]))
    new_only = new_df[~new_df[KEY_COL].isin(base_keys)]
    if not new_only.empty:
        # Keep only columns that are already in base (to avoid exploding schema)
        keep_cols = [c for c in base_df.columns if c in new_only.columns]
        if KEY_COL not in keep_cols:
            keep_cols = [KEY_COL] + keep_cols
        to_append = new_only[keep_cols].copy()
        # Add any missing columns to to_append to match base columns
        for c in base_df.columns:
            if c not in to_append.columns:
                to_append[c] = pd.NA
        # Reorder columns to base order
        to_append = to_append[base_df.columns]
        base_df = pd.concat([base_df, to_append], ignore_index=True)

    return base_df


# Periodic table symbols (case-insensitive matching)
_ELEMENTS: Set[str] = {
    'H','He','Li','Be','B','C','N','O','F','Ne','Na','Mg','Al','Si','P','S','Cl','Ar','K','Ca',
    'Sc','Ti','V','Cr','Mn','Fe','Co','Ni','Cu','Zn','Ga','Ge','As','Se','Br','Kr','Rb','Sr',
    'Y','Zr','Nb','Mo','Tc','Ru','Rh','Pd','Ag','Cd','In','Sn','Sb','Te','I','Xe','Cs','Ba',
    'La','Ce','Pr','Nd','Pm','Sm','Eu','Gd','Tb','Dy','Ho','Er','Tm','Yb','Lu','Hf','Ta','W',
    'Re','Os','Ir','Pt','Au','Hg','Tl','Pb','Bi','Po','At','Rn','Fr','Ra','Ac','Th','Pa','U',
    'Np','Pu','Am','Cm','Bk','Cf','Es','Fm','Md','No','Lr','Rf','Db','Sg','Bh','Hs','Mt','Ds',
    'Rg','Cn','Nh','Fl','Mc','Lv','Ts','Og'
}


def _is_element_col(name: str) -> bool:
    # Exact match against element symbols (case-insensitive). Also accept full uppercase of valid symbols
    n = name.strip()
    if not n:
        return False
    # Direct symbol match ignoring case
    if n.capitalize() in _ELEMENTS:
        return True
    # Some users may use all-uppercase (e.g., "AL", "TI")
    if n.upper() in {s.upper() for s in _ELEMENTS}:
        return True
    return False


def add_new_columns(base_df: pd.DataFrame, new_df: pd.DataFrame, mode: str = 'auto') -> pd.DataFrame:
    """
    mode: 'auto' -> add only element-like new columns
          'all'  -> add all new columns from new_df not present in base_df
          'none' -> add nothing
    The values are filled by matching source_folder.
    """
    if mode == 'none':
        return base_df

    base_df = base_df.copy()
    new_df = new_df.copy()
    base_df[KEY_COL] = normalize_key(base_df[KEY_COL])
    new_df[KEY_COL] = normalize_key(new_df[KEY_COL])

    base_cols = list(base_df.columns)
    new_cols = [c for c in new_df.columns if c not in base_cols and c != KEY_COL]
    if not new_cols:
        return base_df

    if mode == 'auto':
        new_cols = [c for c in new_cols if _is_element_col(str(c))]
        if not new_cols:
            return base_df
    elif mode == 'all':
        pass
    else:
        # Fallback safe behavior
        new_cols = []

    # Build index for fast lookup
    new_idx = new_df.set_index(KEY_COL)

    # Ensure key is unique in new (keep first)
    new_idx = new_idx[~new_idx.index.duplicated(keep='first')]

    # For each new column, create in base and fill values by key
    for col in new_cols:
        if col not in base_df.columns:
            base_df[col] = pd.NA
        # Map aligned values
        s = new_idx.get(col)
        if s is None:
            continue
        base_df.set_index(KEY_COL, inplace=True)
        aligned = s.reindex(base_df.index)
        # Fill where base is missing
        mask_missing = base_df[col].apply(lambda x: pd.isna(x) or (isinstance(x, str) and x.strip()==''))
        base_df.loc[mask_missing, col] = aligned.loc[mask_missing]
        base_df.reset_index(inplace=True)

    return base_df


def main() -> int:
    global TARGET_COLS
    p = argparse.ArgumentParser(description="Merge new data by source_folder into results-csv.xlsx (adds element columns like Ti, Cu, Al)")
    p.add_argument("new_file", help="Path to new XLSX file containing updates, e.g. new_result.xlsx")
    p.add_argument("--out", default=str(BASE_XLSX), help="Output Excel path (default: results-csv.xlsx)")
    p.add_argument("--cols", nargs="*", default=TARGET_COLS,
                   help="Target columns in base to update from new file (default: 合金成分)")
    p.add_argument("--elements", choices=["auto","all","none"], default="auto",
                   help="Add new columns from new file: auto=only element symbols (Ti/Al/Cu...), all=all new cols, none=skip")
    args = p.parse_args()

    new_path = Path(args.new_file)
    if not new_path.exists():
        print(f"新文件不存在: {new_path}", file=sys.stderr)
        return 2

    try:
        # Let pandas pick engine for xlsx; fall back to openpyxl if needed
        try:
            new_df = pd.read_excel(new_path)
        except Exception:
            new_df = pd.read_excel(new_path, engine="openpyxl")
    except Exception as e:
        print(f"读取新文件失败: {e}", file=sys.stderr)
        return 3

    try:
        base_df = read_base_df()
    except Exception as e:
        print(f"读取基准文件失败: {e}", file=sys.stderr)
        return 4

    # Override target columns if provided
    if args.cols:
        TARGET_COLS = args.cols

    merged = merge_new(base_df, new_df)
    # Then add new columns (like elements) and fill values by key
    merged = add_new_columns(merged, new_df, mode=args.elements)

    # Write to .xlsx with UTF-8-compatible writer
    out_path = Path(args.out)
    try:
        merged.to_excel(out_path, index=False, sheet_name=SHEET_NAME, engine="openpyxl")
    except Exception as e:
        print(f"写出结果失败: {e}", file=sys.stderr)
        return 5

    print(f"合并完成: {out_path}  行数={len(merged)} 列数={len(merged.columns)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
