# Watch for new files and send to LinuxLab 
import os
from dotenv import load_dotenv
import time
import paramiko
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

load_dotenv()
DIR = os.getenv("DIR")
LOCAL_PARQUET_DIR = f"{DIR}data/parquet/"
LOCAL_UPLOADED_DIR = f"{DIR}data/uploaded/"

SSH_HOST = os.getenv("HOST_URL")     
SSH_PORT = 22
SSH_USER = os.getenv("USER_ID")       
SSH_PASS = os.getenv("LINUX_LAB_PSWD")   
REMOTE_LANDING_DIR = F"/home/compute/{SSH_USER}/PolyMarket/data"

class ParquetSenderHandler(FileSystemEventHandler):
    def __init__(self):
        super().__init__()
        self._ensure_dirs()
        self.sftp = None
        self.ssh = None
        self._connect_ssh()

        if not SSH_PASS:
            print("Password not found.")
            return
            
        self._connect_ssh()

    def _ensure_dirs(self):
        if not os.path.exists(LOCAL_UPLOADED_DIR):
            os.makedirs(LOCAL_UPLOADED_DIR)

    def _connect_ssh(self):
        try:
            if self.ssh:
                self.ssh.close()
            
            print(f"Connecting to {SSH_HOST}")
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            self.ssh.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)
            self.sftp = self.ssh.open_sftp()
            
            try:
                self.sftp.stat(REMOTE_LANDING_DIR)
            except IOError:
                self.sftp.mkdir(REMOTE_LANDING_DIR)
                
            print("Connected successfully, awaiting files")
        except Exception as e:
            print(f"Error: LinuxLab connection failed: {e}")

    def ship_file(self, filepath):
        filename = os.path.basename(filepath)
        remote_path = os.path.join(REMOTE_LANDING_DIR, filename).replace("\\", "/")

        #DAG will fetch all Parquet files, we do not want it to fetch in-progress files
        remote_tmp_path = remote_path + ".tmp"
        local_archive_path = os.path.join(LOCAL_UPLOADED_DIR, filename)
        

        # Wait until Pandas has completely released the file lock
        time.sleep(1)

        try:
            print(f"\nUploading {filename} to LinuxLab")
            # Upload
            self.sftp.put(filepath, remote_tmp_path)
            self.sftp.rename(remote_tmp_path, remote_path)
            # Archive locally so we don't upload it twice
            os.rename(filepath, local_archive_path)
            print(f"Successfully shipped and archived {filename}")
            
        except OSError as e:
            print(f"ERROR: Network: {e}. Attempting reconnect")
            self._connect_ssh()
            try:
                self.sftp.put(filepath, remote_tmp_path)
                self.sftp.rename(remote_tmp_path, remote_path)

                os.rename(filepath, local_archive_path)
                print(f"Recovered! Successfully shipped {filename}")
            except Exception as retry_e:
                print(f"ERROR: Failed to upload {filename} after retry: {retry_e}")
                
        except Exception as e:
            print(f"Error processing {filename}: {e}")

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.parquet'):
            self.ship_file(event.src_path)

def process_existing_files(handler):
    for filename in os.listdir(LOCAL_PARQUET_DIR):
        if filename.endswith(".parquet"):
            filepath = os.path.join(LOCAL_PARQUET_DIR, filename)
            handler.ship_file(filepath)

if __name__ == "__main__":
    if not os.path.exists(LOCAL_PARQUET_DIR):
        os.makedirs(LOCAL_PARQUET_DIR)

    handler = ParquetSenderHandler() 
    
    print("Checking for existing backlog")
    process_existing_files(handler)

    observer = Observer()
    observer.schedule(handler, LOCAL_PARQUET_DIR, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1) 
    except KeyboardInterrupt:
        print("\nShutting down")
        observer.stop()
        if handler.ssh:
            handler.ssh.close()
            
    observer.join()

