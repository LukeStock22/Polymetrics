import csv

INPUT_CSV = "polymarket_leaderboard_users_async.csv"
OUTPUT_TXT = "wallets_only.txt"

print(f"Extracting wallets from {INPUT_CSV}")

with open(INPUT_CSV, "r", encoding="utf-8") as infile, open(OUTPUT_TXT, "w", encoding="utf-8") as outfile:
     
    reader = csv.DictReader(infile)
    count = 0
    for row in reader:
        wallet = row.get("proxyWallet")
        if wallet:
            outfile.write(wallet + "\n")
            count += 1

print(f"Done! Extracted {count} wallets to {OUTPUT_TXT}")