#!/usr/bin/env python3
"""
Quick script to extract first N rows for testing
"""

import pandas as pd
import sys

def create_test_file(input_file, num_rows=100):
    """Extract first N rows to create a test file"""
    
    print(f"Reading {input_file}...")
    
    # Try reading as CSV first
    try:
        df = pd.read_csv(input_file, encoding='utf-8', dtype=str)
    except:
        try:
            df = pd.read_csv(input_file, encoding='utf-8-sig', dtype=str)
        except:
            try:
                df = pd.read_csv(input_file, encoding='cp1252', dtype=str)
            except:
                # Try Excel
                df = pd.read_excel(input_file, dtype=str)
    
    print(f"Total rows: {len(df)}")
    
    # Extract first N rows
    df_test = df.head(num_rows)
    
    # Save as CSV
    output_file = f"test_{num_rows}_rows.csv"
    df_test.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"[OK] Created test file: {output_file}")
    print(f"[OK] Contains {len(df_test)} rows")
    print(f"\nNow run:")
    print(f"   python enrich_hotels.py {output_file}")
    
    return output_file

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python create_test_file.py your_file.csv [num_rows]")
        print("Example: python create_test_file.py contacts_FINAL_20260205_222438.csv 50")
        sys.exit(1)
    
    input_file = sys.argv[1]
    num_rows = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    
    create_test_file(input_file, num_rows)
