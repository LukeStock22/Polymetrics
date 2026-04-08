import subprocess
import time
import sys
import os

# we may or may not want to wait between runs. 
# I have not yet tested this file. I will test it when I reach the end of the wallets 
CYCLE_DELAY_SECONDS = 0 

def run_script(script_name):
    print(f"\nStarting {script_name}\n")
    try:
        subprocess.run([sys.executable, script_name], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {script_name} failed with code {e.returncode}")
        return False
    except KeyboardInterrupt:
        print(f"\nOrchestrator caught KeyboardInterrupt during {script_name}.")
        return False

def main_loop():
    iteration_num = 1
    
    while True:
        print(f"\niteration #{iteration_num}\n")
        
        #this just exists cause sometimes I start the script from the vscode play button
        #and then I cannot cmd c to safely terminate it. Not sure if it works though

        if os.path.exists("progress/STOP.txt"):
            print("STOP.txt. Orchestrator KILLED.")
            break

        # Fetch new users - appends to CSV
        if not run_script("fetch_users.py"): break
        
        # Extract wallets - overwrites the text file with all current and new wallets
        if not run_script("extract_wallets.py"): break
        
        # Fetch NEW transactions - write NEW Parquets and update EXISTING JSON timestamps
        if not run_script("fetch_user_transaction_history.py"): break
        
        print(f"\nIteration #{iteration_num} completed. HOORAY")
        print(f"Delaying for {CYCLE_DELAY_SECONDS / 3600:.2f} hours")
        
        try:
            time.sleep(CYCLE_DELAY_SECONDS)
        except KeyboardInterrupt:
            print("\nTerminated")
            break
            
        iteration_num += 1

if __name__ == "__main__":
    main_loop()