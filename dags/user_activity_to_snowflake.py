from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
import glob #Glob is used to get parquet files from folder
import shutil #shutil is used to move files across folders

SNOWFLAKE_CONN_ID = "Snowflake_Default" 
STAGE_NAME = "@POLYMARKET_STAGE/user_activity"
LANDING_DIR = "/home/compute/andelman.w/PolyMarket/data/"
ARCHIVE_DIR = "/home/compute/andelman.w/PolyMarket/data/archive/"

with DAG(
    dag_id='polymarket_activity_pipeline_dag',
    schedule='@hourly', 
    catchup=False,
    tags=['polymarket'],
) as dag:

    @task
    def snowflake_objects():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        sql_queries = [
            #Create stage if none exists
            "CREATE STAGE IF NOT EXISTS POLYMARKET_STAGE;",

            #create file format, handle nan issue
            """
            CREATE FILE FORMAT IF NOT EXISTS polymarket_parq_format
                TYPE = PARQUET
                NULL_IF = ('nan', 'NaN', '', 'NULL', 'null');
            """,

            #create table:
            """
            CREATE TABLE IF NOT EXISTS CURATED_POLYMARKET_USER_ACTIVITY (
                timestamp BIGINT,
                proxyWallet VARCHAR,
                type VARCHAR,
                conditionId VARCHAR,
                asset VARCHAR,
                side VARCHAR,
                outcome VARCHAR,
                outcomeIndex VARCHAR,
                price FLOAT,
                size FLOAT,
                usdcSize FLOAT,
                transactionHash VARCHAR,
                _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
            """,
        ]

        for query in sql_queries:
            hook.run(query)
        print("Snowflake is ready.")
    
    @task
    def upload_files_to_stage():
        if not os.path.exists(ARCHIVE_DIR):
            os.makedirs(ARCHIVE_DIR)

        # get parquet files w/ glob
        parquet_files = glob.glob(os.path.join(LANDING_DIR, "*.parquet"))

        if not parquet_files:
            print("No new parquet files found. Skip upload.")
            return False

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        #upload files to stage
        for file in parquet_files:
            filename = os.path.basename(file)
            print(f"Uploading {filename} to Snowflake Stage")
            put_query = f"PUT file://{file} {STAGE_NAME} AUTO_COMPRESS=TRUE;"
            hook.run(put_query)
        
        #other tasks will use file list
        return parquet_files
    
    @task
    def process_data(files_uploaded: list):

        if not files_uploaded:
            print("No files were uploaded. Skipping processing.")
            return files_uploaded

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        # Atomic Operation
        #1. Create temporary table to be dropped on COMMIT
        #2. COPY INTO temp table from stage, use $1 column notation to get Parquet Cols, TRY_CAST is overkill (data should be clean) but why not
        #3. If any file cant be found, abort the load operation
        #4. MERGE INTO merges data from temp (source) to curated table (target). If data is already in the table, ignore, else add. This way adding parquets twice isnt a concern
        #5. REMOVE deletes the internal stage. We dont need it anymore, and on rerun we would MERGE all of the old files, which would be fine but unneccessary. 
        
        sql_script = f"""
        BEGIN;
        
        CREATE TEMPORARY TABLE TEMP_POLYMARKET_RAW_ACTIVITY (
            timestamp BIGINT, proxyWallet VARCHAR, type VARCHAR, conditionId VARCHAR, 
            asset VARCHAR, side VARCHAR, outcome VARCHAR, outcomeIndex VARCHAR, 
            price FLOAT, size FLOAT, usdcSize FLOAT, transactionHash VARCHAR
        );

        COPY INTO TEMP_POLYMARKET_RAW_ACTIVITY 
        FROM (
            SELECT 
                TRY_CAST($1:timestamp::VARCHAR AS BIGINT), $1:proxyWallet::VARCHAR,
                $1:type::VARCHAR, $1:conditionId::VARCHAR, $1:asset::VARCHAR,
                $1:side::VARCHAR, $1:outcome::VARCHAR, $1:outcomeIndex::VARCHAR,
                TRY_CAST($1:price::VARCHAR AS FLOAT), TRY_CAST($1:size::VARCHAR AS FLOAT),
                TRY_CAST($1:usdcSize::VARCHAR AS FLOAT), $1:transactionHash::VARCHAR
            FROM {STAGE_NAME}
        )
        FILE_FORMAT = (FORMAT_NAME = polymarket_parq_format)
        ON_ERROR = 'ABORT_STATEMENT'; 

        MERGE INTO CURATED_POLYMARKET_USER_ACTIVITY target
        USING TEMP_POLYMARKET_RAW_ACTIVITY source
        ON target.transactionHash = source.transactionHash
           AND target.timestamp = source.timestamp
           AND target.type = source.type
           AND target.conditionId = source.conditionId
           AND target.outcomeIndex = source.outcomeIndex
           AND target.size = source.size
        WHEN NOT MATCHED THEN
            INSERT (
                timestamp, proxyWallet, type, conditionId, asset, side, 
                outcome, outcomeIndex, price, size, usdcSize, transactionHash
            )
            VALUES (
                source.timestamp, source.proxyWallet, source.type, source.conditionId, source.asset, source.side, 
                source.outcome, source.outcomeIndex, source.price, source.size, source.usdcSize, source.transactionHash
            );

        
        REMOVE {STAGE_NAME};

        COMMIT;
        """
        
        try:
            hook.run(sql_script)
            print("Data successfully processed and merged.")
            return files_uploaded
        except Exception as e:
            hook.run("ROLLBACK;")
            print("process_data failed. Transaction rolled back.")
            raise e


    @task
    def archive_processed_files(files_uploaded: list):
        #Archive uploaded files. These are actually safe to delete now if storage is a concern.
        if not files_uploaded:
            return
        if not os.path.exists(ARCHIVE_DIR):
            os.makedirs(ARCHIVE_DIR)
        
        for file in files_uploaded:
            if os.path.exists(file):
                filename = os.path.basename(file)
                shutil.move(file, os.path.join(ARCHIVE_DIR, filename))
                print(f"Archived file: {filename}")
        
    snowflake_setup_task = snowflake_objects()
    upload_task = upload_files_to_stage()
    process_data_task = process_data(upload_task)
    archive_data_task = archive_processed_files(process_data_task)

    snowflake_setup_task >> upload_task >> process_data_task >> archive_data_task
