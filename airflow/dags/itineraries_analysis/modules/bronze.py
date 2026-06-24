import os
from airflow.exceptions import AirflowException
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from itineraries_analysis.utils import get_file_path, log_info, log_error
from itineraries_analysis.config import BRONZE_HDFS_PATH, LOCAL_DATA_PATH
from pyhive import hive

def process_bronze_layer(**kwargs):
    try:
        webhdfs_hook = WebHDFSHook(webhdfs_conn_id='webhdfs_default')
        client = webhdfs_hook.get_conn()
        
        local_file_path = get_file_path('itineraries.csv')
        
        log_info(f"Contents of /opt/airflow/data: {os.listdir('/opt/airflow/data')}")

        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"The file {local_file_path} does not exist locally.")
        
        log_info(f"Checking file in HDFS: {BRONZE_HDFS_PATH}")
        
        hdfs_path = '/user/airflow/data/bronze/itineraries/itineraries.csv'
        parent_dir = os.path.dirname(hdfs_path)
        
        log_info(f"HDFS path: {hdfs_path}")
        log_info(f"Parent directory: {parent_dir}")

        try:
            client.status(parent_dir)
        except:
            log_info(f"Creating directory: {parent_dir}")
            client.makedirs(parent_dir)
        
        try:
            file_status = client.status(hdfs_path)
            log_info(f"File exists in HDFS. Status: {file_status}")
        except:
            log_info(f"File not found in HDFS. Uploading from {local_file_path} to {hdfs_path}")
            with open(local_file_path, 'rb') as local_file:
                client.write(hdfs_path, data=local_file)
            log_info("File uploaded successfully")

        with client.read(hdfs_path) as reader:
            content = reader.read(1024).decode('utf-8')
            log_info(f"First 1024 bytes of HDFS file:\n{content}")

        create_table_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS bronze_itineraries (
            legId STRING,
            searchDate STRING,
            flightDate STRING,
            startingAirport STRING,
            destinationAirport STRING,
            fareBasisCode STRING,
            travelDuration STRING,
            elapsedDays INT,
            isBasicEconomy BOOLEAN,
            isRefundable BOOLEAN,
            isNonStop BOOLEAN,
            baseFare FLOAT,
            totalFare FLOAT,
            seatsRemaining INT,
            totalTravelDistance INT,
            segmentsDepartureTimeEpochSeconds BIGINT,
            segmentsDepartureTimeRaw STRING,
            segmentsArrivalTimeEpochSeconds BIGINT,
            segmentsArrivalTimeRaw STRING,
            segmentsArrivalAirportCode STRING,
            segmentsDepartureAirportCode STRING,
            segmentsAirlineName STRING,
            segmentsAirlineCode STRING,
            segmentsEquipmentDescription STRING,
            segmentsDurationInSeconds INT,
            segmentsDistance INT,
            segmentsCabinCode STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '/user/airflow/data/bronze/itineraries'
        TBLPROPERTIES ("skip.header.line.count"="1")
        """

        log_info("Attempting to connect to HiveServer2")
        try:
            conn = hive.connect(
                host='hive-server',
                port=10000,
                username='hive',
                database='default'
            )
            cursor = conn.cursor()
            log_info("Successfully connected to HiveServer2")
            
            log_info("Executing Hive query to create bronze_itineraries table")
            cursor.execute(create_table_sql)
            log_info("Hive query executed successfully")

            verify_query = "SELECT * FROM bronze_itineraries LIMIT 10"
            cursor.execute(verify_query)
            results = cursor.fetchall()
            if results:
                log_info(f"Sample data from bronze_itineraries: {results}")
            else:
                log_info("No data found in bronze_itineraries table")

            cursor.execute("SELECT COUNT(*) FROM bronze_itineraries")
            count = cursor.fetchone()[0]
            log_info(f"Number of rows in bronze_itineraries: {count}")

            cursor.close()
            conn.close()
        except Exception as e:
            log_error(f"Error executing Hive query: {str(e)}")
            raise

        log_info("Bronze layer Hive table created and populated with HDFS data")

        return BRONZE_HDFS_PATH
    except Exception as e:
        log_error(f"Error in process_bronze_layer: {str(e)}")
        raise AirflowException(f"Bronze layer processing failed: {str(e)}")