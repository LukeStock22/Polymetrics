#collect list of names
#search by name
#fetch address

import os
import asyncio
import aiohttp
import csv
import time


LEADERBOARD_API = "https://data-api.polymarket.com/v1/leaderboard"
PAGE_SIZE = 50
MAX_OFFSET = 10050
OUTPUT_CSV = "polymarket_leaderboard_users_async.csv"
STATE_FILE = "progress.txt"
WORKER_COUNT = 80

CHARS = "abcdefghijklmnopqrstuvwxyz0123456789.-"

async def save_progress(frontier: set, state_lock: asyncio.Lock):
    async with state_lock:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            for item in frontier:
                f.write(item + "\n")

async def worker(queue, session, csv_writer, seen_wallets, csv_lock, frontier, state_lock):
    while True:
        query_str = await queue.get()
        offset = 0
        hit_api_limit = False
        
        while True:
            params = {
                "timePeriod": "all",
                "orderBy": "PNL",
                "limit": PAGE_SIZE,
                "offset": offset,
                "category": "overall",
                "userName": query_str
            }
            
            try:
                async with session.get(LEADERBOARD_API, params=params, timeout=15) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(2)
                        continue
                    data = await resp.json()
            except Exception as e:
                # Network error -  retry
                await asyncio.sleep(2)
                continue

            if not data:
                break

            new_users = []
            for user in data:
                wallet = user.get("proxyWallet")
                if wallet and wallet not in seen_wallets:
                    seen_wallets.add(wallet)
                    new_users.append(user)

            if new_users:
                async with csv_lock:
                    for u in new_users:
                        row = {k: u.get(k, "") for k in csv_writer.fieldnames}
                        csv_writer.writerow(row)

            if len(data) < PAGE_SIZE:
                break
                
            offset += PAGE_SIZE
            
            if offset > MAX_OFFSET:
                hit_api_limit = True
                break

        
        async with state_lock:
            if query_str in frontier:
                frontier.remove(query_str)
            
            if hit_api_limit:
                print(f"Hit API limit for '{query_str}'. Expanding")
                for c in CHARS:
                    new_prefix = query_str + c
                    frontier.add(new_prefix)
                    queue.put_nowait(new_prefix)
        
        await save_progress(frontier, state_lock)
        
        queue.task_done()

async def main():
    seen_wallets = set()
    frontier = set() #track curr bound
    queue = asyncio.Queue()
    
    csv_lock = asyncio.Lock()
    state_lock = asyncio.Lock()
    
    # LOAD PREVIOUS WALLETS 
    if os.path.exists(OUTPUT_CSV):
        print(f"Found existing CSV at {OUTPUT_CSV}. Loading known wallets")
        with open(OUTPUT_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                wallet = row.get("proxyWallet")
                if wallet:
                    seen_wallets.add(wallet)
        print(f"Loaded {len(seen_wallets)} wallets")

    # LOAD PREVIOUS QUEUE STATE 
    if os.path.exists(STATE_FILE):
        print(f"Found existing state file at {STATE_FILE}. Resuming")
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            for line in f:
                prefix = line.strip()
                if prefix:
                    frontier.add(prefix)
                    queue.put_nowait(prefix)
        print(f"Loaded {len(frontier)} frontier searches from prev run.")
    else:
        print("No state file. Starting fresh")
        for c in CHARS:
            frontier.add(c)
            queue.put_nowait(c)

    fieldnames = [
        "rank", "proxyWallet", "userName", "xUsername", 
        "verifiedBadge", "vol", "pnl", "profileImage"
    ]

    print("Starting workers")
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        if f.tell() == 0:
            writer.writeheader()

        connector = aiohttp.TCPConnector(limit=WORKER_COUNT)
        async with aiohttp.ClientSession(connector=connector) as session:
            
            workers = [
                asyncio.create_task(worker(queue, session, writer, seen_wallets, csv_lock, frontier, state_lock))
                for _ in range(WORKER_COUNT)
            ]

            await queue.join()

            for w in workers:
                w.cancel()

    # Clean up the state file when done
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

    print(f"\nFinished! Fetched {len(seen_wallets)} unique users.")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(main())