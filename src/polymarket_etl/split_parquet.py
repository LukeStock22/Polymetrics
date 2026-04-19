import time
import json
import pandas as pd
import os
import re
from dotenv import load_dotenv

load_dotenv()

DIR = os.getenv("DIR")

#order matters
INPUT_FILES = [
    f"{DIR}data/polymarket_all_activity_complete.csv",
    f"{DIR}data/polymarket_all_activity_complete_2.csv"
]
CHUNK_SIZE = 5_000_000 
BLOAT_COLS = ["timestamp_iso", "title"]
OUTPUT_JSON = f"{DIR}progress/wallet_tracking.json"
PARQUET_DIR = f"{DIR}data/parquet/"

def get_next_chunk_index():
    if not os.path.exists(PARQUET_DIR):
        os.makedirs(PARQUET_DIR)
        return 1
    files = [f for f in os.listdir(PARQUET_DIR) if f.endswith(".parquet")]
    if not files:
        return 1

    indices = [int(re.findall(r'\d+', f)[-1]) for f in files]
    return max(indices) + 1

def load_existing_watermarks():
    if os.path.exists(OUTPUT_JSON):
        print(f"Loading watermarks from {OUTPUT_JSON}")
        with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def process_all_files():
    wallet_watermarks = load_existing_watermarks()
    chunk_index = get_next_chunk_index()

    row_buffer = pd.DataFrame()
    total_processed_rows = 0

    for input_path in INPUT_FILES:
        if not os.path.exists(input_path):
            print(f"Skipping {input_path} - File not found")
            continue

        print(f"\nProcessing {os.path.basename(input_path)}")
        csv_reader = pd.read_csv(input_path, chunksize=1_000_000, dtype=str)

        for csv_chunk in csv_reader:
            csv_chunk.drop(columns=[c for c in BLOAT_COLS if c in csv_chunk.columns], inplace=True)
            csv_chunk['timestamp'] = pd.to_numeric(csv_chunk['timestamp'], errors='coerce').fillna(0).astype('int64')
            csv_chunk['price'] = pd.to_numeric(csv_chunk['price'], errors='coerce').fillna(0.0).astype('float64')
            csv_chunk['size'] = pd.to_numeric(csv_chunk['size'], errors='coerce').fillna(0.0).astype('float64')

            if 'usdcSize' in csv_chunk.columns:
                csv_chunk['usdcSize'] = pd.to_numeric(csv_chunk['usdcSize'], errors='coerce').fillna(0.0).astype('float64')

            str_cols = ['proxyWallet', 'type', 'conditionId', 'asset', 'side', 'outcome', 'outcomeIndex', 'transactionHash']
            for col in str_cols:
                if col in csv_chunk.columns:
                    csv_chunk[col] = csv_chunk[col].astype(str)
            #Watermarks
            latest_idx = csv_chunk.groupby('proxyWallet')['timestamp'].idxmax()
            chunk_maxes = csv_chunk.loc[latest_idx, ['proxyWallet', 'timestamp', 'transactionHash']]

            for _, row in chunk_maxes.iterrows():
                w = row['proxyWallet']
                ts = int(row['timestamp']) 
                tx = row['transactionHash']
                
                existing = wallet_watermarks.get(w)  
                if not existing or ts > existing[0]:
                    wallet_watermarks[w] = (ts, tx)

            row_buffer = pd.concat([row_buffer, csv_chunk], ignore_index=True)

            #buffer management
            while len(row_buffer) >= CHUNK_SIZE:
                parquet_chunk = row_buffer.iloc[:CHUNK_SIZE]
                row_buffer = row_buffer.iloc[CHUNK_SIZE:] 
                
                out_name = f"{PARQUET_DIR}activity_chunk_{chunk_index:03d}.parquet"
                parquet_chunk.to_parquet(out_name, engine='pyarrow', index=False)
                
                print(f"Saved {out_name} | Total Rows: {total_processed_rows + CHUNK_SIZE:,}")
                chunk_index += 1 
                total_processed_rows += CHUNK_SIZE


    if not row_buffer.empty:
        out_name = f"{PARQUET_DIR}activity_chunk_{chunk_index:03d}.parquet"
        row_buffer.to_parquet(out_name, engine='pyarrow', index=False)
        print(f"Saved final partial chunk {out_name} ({len(row_buffer):,} rows)")

    print(f"\nUpdating {OUTPUT_JSON} with {len(wallet_watermarks):,} unique wallets")
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(wallet_watermarks, f, indent=4)
    print(f"Done!")

if __name__ == "__main__":
    process_all_files()