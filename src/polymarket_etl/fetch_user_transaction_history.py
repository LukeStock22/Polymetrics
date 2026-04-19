# for every user in progress/wallets_only.txt
# get complete transaction history
# Guarantees atomic writes across Parquet, JSON, and progress text files.

import os
import time
import asyncio
import aiohttp
import json
import re
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://data-api.polymarket.com"
POSITIONS_ENDPOINTS = ["/closed-positions", "/positions"]
ACTIVITY_ENDPOINT = "/activity"

POS_LIMIT = 50
POS_MAX_OFFSET = 100000
ACT_LIMIT = 500
ACT_MAX_OFFSET = 3000

DIR = os.getenv("DIR")
INPUT_TXT = f"{DIR}progress/wallets_only.txt"
STATE_FILE = f"{DIR}progress/activity_progress.txt"
JSON_TRACKER = f"{DIR}progress/wallet_tracking.json"
PARQUET_DIR = f"{DIR}data/parquet/"
STOP_FILE = f"{DIR}progress/STOP.txt"
 
WORKER_COUNT = 40
CHUNK_SIZE = 5_000_000

class PerformanceStats:
    def __init__(self):
        self.total_requests = 0
        self.start_time = time.monotonic()
        
    def increment(self): 
        self.total_requests += 1
  
stats = PerformanceStats()
 
class TokenBucket:
    def __init__(self, rate_per_sec):
        self.rate = rate_per_sec
        self.allowance = rate_per_sec
        self.last_check = time.monotonic()
        self.lock = asyncio.Lock()

    async def wait(self):
        sleep_time = 0
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_check
            self.last_check = now
            self.allowance += elapsed * self.rate
            
            if self.allowance > self.rate:
                self.allowance = self.rate
                
            if self.allowance >= 1.0:
                self.allowance -= 1.0
            else:
                sleep_time = (1.0 - self.allowance) / self.rate
                self.allowance = 0.0
                self.last_check = now + sleep_time 
                
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

async def monitor_rps(queue):
    while True:
        await asyncio.sleep(60) 
        elapsed = time.monotonic() - stats.start_time
        rps = stats.total_requests / elapsed if elapsed > 0 else 0
        #heartbeat msgs with stats
        print(f"MONITOR: Elapsed: {elapsed:.0f}s | Total Reqs: {stats.total_requests} | Avg Speed: {rps:.2f} RPS | Wallets Left: {queue.qsize()}")
 
pos_open_limiter = TokenBucket(14)  
pos_closed_limiter = TokenBucket(14) 
act_limiter = TokenBucket(95)  

class ParquetChunkManager:
    def __init__(self):
        self.buffer = []
        self.completed_wallets_queue = [] 
        self.watermarks = {}
        self.chunk_index = 26
        self.lock = asyncio.Lock()
        
        if not os.path.exists(PARQUET_DIR):
            os.makedirs(PARQUET_DIR)
            
        self._initialize_state()

    def _initialize_state(self):
        if os.path.exists(JSON_TRACKER):
            with open(JSON_TRACKER, 'r', encoding='utf-8') as f:
                self.watermarks = json.load(f)
                
        files = [f for f in os.listdir(PARQUET_DIR) if f.endswith(".parquet")]

        if files:
            indices = [int(re.findall(r'\d+', f)[-1]) for f in files]
            self.chunk_index = max(indices)
            
            latest_file = os.path.join(PARQUET_DIR, f"activity_chunk_{self.chunk_index:03d}.parquet")
            print(f"Loading partial chunk {latest_file} into mem")
            df = pd.read_parquet(latest_file)
            self.buffer = df.to_dict('records')
            print(f"Resumed with {len(self.buffer):,} rows in buffer.")

    def _write_all_sync(self, data, path, wallets_to_mark):
        
        try:
            df = pd.DataFrame(data)
            
            if not df.empty:
                df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce').fillna(0).astype('int64')
                df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0.0).astype('float64')
                df['size'] = pd.to_numeric(df['size'], errors='coerce').fillna(0.0).astype('float64')
                df['usdcSize'] = pd.to_numeric(df['usdcSize'], errors='coerce').fillna(0.0).astype('float64')
                
                str_cols = ['proxyWallet', 'type', 'conditionId', 'asset', 'side', 'outcome', 'outcomeIndex', 'transactionHash']
                for col in str_cols:
                    if col in df.columns:
                        df[col] = df[col].astype(str)

                df.to_parquet(path, engine='pyarrow', index=False)
            
            with open(JSON_TRACKER, 'w', encoding='utf-8') as f:
                json.dump(self.watermarks, f, indent=4)
                
            if wallets_to_mark:
                with open(STATE_FILE, "a", encoding="utf-8") as f:
                    for w in wallets_to_mark:
                        f.write(w + "\n")
                        
        except Exception as e:
            print(f"\nALERT: Failed atomic save on {path}: {e}")
            raise e

    async def add_activities(self, activities, wallet, max_ts, max_hash):
        async with self.lock:
            if activities:
                self.buffer.extend(activities)
            
            self.completed_wallets_queue.append(wallet)
                
            if wallet and max_ts > 0:
                existing = self.watermarks.get(wallet)
                if not existing or max_ts > existing[0]:
                    self.watermarks[wallet] = (max_ts, max_hash)

            # Flush if buffer EXCEEDS chunk size 
            # We dump the whole thing so wallets don't split
            # This doesn't really matter - parquets can be greater than 5,000,000 rows AND wallet transactions can be split across files
            # It is just important that it gets written somewhere and not lost.
            if len(self.buffer) >= CHUNK_SIZE:
                chunk_data = self.buffer
                wallets_to_mark = self.completed_wallets_queue
                
                # Clear mem
                self.buffer = []
                self.completed_wallets_queue = []
                
                out_name = os.path.join(PARQUET_DIR, f"activity_chunk_{self.chunk_index:03d}.parquet")
                print(f"\n Saving {len(chunk_data):,} rows and {len(wallets_to_mark):,} wallets to {out_name}")
                
                await asyncio.to_thread(self._write_all_sync, chunk_data, out_name, wallets_to_mark)
                
                self.chunk_index += 1

    async def flush_remaining(self):
        #Called at the end to save that final incomplete chunk
        async with self.lock:
            if self.buffer or self.completed_wallets_queue:
                out_name = os.path.join(PARQUET_DIR, f"activity_chunk_{self.chunk_index:03d}.parquet")
                print(f"Saving final partial chunk ({len(self.buffer):,} rows) to {out_name}")
                await asyncio.to_thread(self._write_all_sync, self.buffer, out_name, self.completed_wallets_queue)

chunk_manager = ParquetChunkManager() 


async def fetch_condition_ids(session, wallet):
    condition_ids = set()
    for endpoint in POSITIONS_ENDPOINTS:
        limiter = pos_closed_limiter if "closed" in endpoint else pos_open_limiter
        offset = 0
        while offset <= POS_MAX_OFFSET:
            backoff = 10
            data = None
            while data is None: 
                await limiter.wait()
                params = {"user": wallet, "limit": POS_LIMIT, "offset": offset}
                try:
                    stats.increment()
                    async with session.get(BASE_URL + endpoint, params=params, timeout=30) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                        elif resp.status in [502, 503, 504, 500, 429]:
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 240)
                        else:
                            data = [] 
                except Exception:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 240)
            if not data:
                break
            for pos in data:
                cid = pos.get("conditionId")
                if cid:
                    condition_ids.add(cid)
            if len(data) < POS_LIMIT:
                break
            offset += POS_LIMIT 
    return condition_ids

async def fetch_activity(session, params, stop_ts=0, stop_hash=""):
    activities = []
    offset = 0 
    stop_trigger = False

    while offset <= ACT_MAX_OFFSET and not stop_trigger:
        data = None
        backoff = 10
        while data is None:
            await act_limiter.wait()
            curr_params = {**params, "limit": ACT_LIMIT, "offset": offset, "sortBy": "TIMESTAMP", "sortDirection": "DESC"}
            try:
                stats.increment()
                async with session.get(BASE_URL + ACTIVITY_ENDPOINT, params=curr_params, timeout=30) as resp:
                    if resp.status == 200: 
                        data = await resp.json()
                    elif resp.status in [502, 503, 504, 500, 429]:
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 240)
                    else:
                        data = []
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 240)
        if not data:
            break
        
        if stop_ts != 0:
            for row in data:
                ts = int(float(row.get("timestamp", 0))) if row.get("timestamp") else 0
                t_hash = row.get("transactionHash", "")
                if ts < stop_ts or (ts == stop_ts and t_hash == stop_hash):
                    stop_trigger = True
                    break
                activities.append(row)
        else:
            activities.extend(data)
        if len(data) < ACT_LIMIT:
            break
        offset += ACT_LIMIT
    return activities

async def process_wallet(wallet, session, chunk_manager):
    seen_keys = set()
    formatted_activities = []
    market_activities = [] 
    
    watermarks = chunk_manager.watermarks.get(wallet, (0, ""))
    last_ts = watermarks[0]
    last_hash = watermarks[1]

    global_activities = await fetch_activity(session, {"user": wallet}, stop_ts=last_ts, stop_hash=last_hash)
    
    if len(global_activities) >= ACT_MAX_OFFSET: 
        print(f"WHALE DETECTED {wallet} ")
        condition_ids = await fetch_condition_ids(session, wallet)
        for cid in condition_ids:
            acts = await fetch_activity(session, {"user": wallet, "market": cid})
            market_activities.extend(acts)
            
    wallet_max_ts = last_ts
    wallet_max_hash = last_hash


    for row in (global_activities + market_activities):
        key = (
            row.get("timestamp"),
            row.get("transactionHash"),
            row.get("type"),
            row.get("conditionId"),
            row.get("outcomeIndex"),
            row.get("size")
        )
        
        if key in seen_keys:
            continue
        seen_keys.add(key)
        
        ts_int = int(float(row.get("timestamp", 0))) if row.get("timestamp") else 0
        
        act = {
            "timestamp": ts_int,
            "proxyWallet": wallet,
            "type": str(row.get("type", "")),
            "conditionId": str(row.get("conditionId", "")),
            "asset": str(row.get("asset", "")),
            "side": str(row.get("side", "")),
            "outcome": str(row.get("outcome", "")),
            "outcomeIndex": str(row.get("outcomeIndex", "")),
            "price": float(row.get("price", 0)) if row.get("price") else 0.0,
            "size": float(row.get("size", 0)) if row.get("size") else 0.0,
            "usdcSize": float(row.get("usdcSize", 0)) if row.get("usdcSize") else 0.0,
            "transactionHash": str(row.get("transactionHash", ""))
        }
        formatted_activities.append(act)

        if ts_int > wallet_max_ts:
            wallet_max_ts = ts_int
            wallet_max_hash = act["transactionHash"]
            
    await chunk_manager.add_activities(formatted_activities, wallet, wallet_max_ts, wallet_max_hash)

async def worker(queue, session, chunk_manager):
    while True: 
        try:
            # get_nowait prevents workers from hanging
            wallet = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
            
        try:
            await process_wallet(wallet, session, chunk_manager)
        except Exception as e:
            print(f"\nALERT: Worker crashed on wallet {wallet}: {e}")
        finally:
            queue.task_done()
            
        if os.path.exists(STOP_FILE):
            print(f"\nALERT: STOP.txt detected! Worker {id(asyncio.current_task())} safely terminating")
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
            break 

async def main():
    completed_wallets = set()
    queue = asyncio.Queue() 
    
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            for line in f:
                completed_wallets.add(line.strip())
        print(f"Skipping {len(completed_wallets)} completed wallets.")
        
    if not os.path.exists(INPUT_TXT):
        print(f"Error: Could not find {INPUT_TXT}")
        return
        
    print(f"Reading target wallets from {INPUT_TXT}")
    with open(INPUT_TXT, "r", encoding="utf-8") as f:
        for line in f:
            wallet = line.strip()
            if wallet and wallet not in completed_wallets:
                queue.put_nowait(wallet)
                
    print(f"Queued {queue.qsize()} wallets for extraction")

    connector = aiohttp.TCPConnector(limit=WORKER_COUNT)
    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [
            asyncio.create_task(worker(queue, session, chunk_manager))
            for _ in range(WORKER_COUNT)
        ]
        monitor_task = asyncio.create_task(monitor_rps(queue))
        
        await queue.join()

        for w in workers:
            w.cancel()
        monitor_task.cancel()

    await chunk_manager.flush_remaining()

    #After we have processed every single user,we delete activity_progress.txt and restart!
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print(f"Deleted {STATE_FILE}. Ready for next cycle.")

    print("Complete!")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nALERT: Ctrl+C detected! Start safe shutdown")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(chunk_manager.flush_remaining())
        loop.close()
        print("\nData successfully written to Parquet. Safe to exit.")