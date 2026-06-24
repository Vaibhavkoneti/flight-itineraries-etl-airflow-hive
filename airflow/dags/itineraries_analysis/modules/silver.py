from airflow.exceptions import AirflowException
from itineraries_analysis.utils import log_info, log_error
from itineraries_analysis.config import SILVER_HDFS_PATH
from pyhive import hive

def process_silver_layer(**kwargs):
    try:
        create_silver_table_sql = f"""
        CREATE TABLE IF NOT EXISTS silver_itineraries
        STORED AS ORC
        LOCATION '{SILVER_HDFS_PATH}'
        AS
        SELECT 
            legId,
            to_date(searchDate) as search_date,
            to_date(flightDate) as flight_date,
            startingAirport,
            destinationAirport,
            fareBasisCode,
            travelDuration,
            elapsedDays,
            isBasicEconomy,
            isRefundable,
            isNonStop,
            baseFare,
            totalFare,
            seatsRemaining,
            totalTravelDistance,
            from_unixtime(segmentsDepartureTimeEpochSeconds) as departure_time,
            segmentsDepartureTimeRaw,
            from_unixtime(segmentsArrivalTimeEpochSeconds) as arrival_time,
            segmentsArrivalTimeRaw,
            segmentsArrivalAirportCode,
            segmentsDepartureAirportCode,
            segmentsAirlineName,
            segmentsAirlineCode,
            segmentsEquipmentDescription,
            segmentsDurationInSeconds,
            segmentsDistance,
            segmentsCabinCode
        FROM bronze_itineraries
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
            
            log_info("Executing Hive query to create silver_itineraries table")
            cursor.execute(create_silver_table_sql)
            log_info("Hive query executed successfully")
            
            verify_query = "SELECT * FROM silver_itineraries LIMIT 10"
            cursor.execute(verify_query)
            results = cursor.fetchall()
            if results:
                log_info(f"Sample data from silver_itineraries: {results}")
            else:
                log_info("No data found in silver_itineraries table")
            
            cursor.execute("SELECT COUNT(*) FROM silver_itineraries")
            count = cursor.fetchone()[0]
            log_info(f"Number of rows in silver_itineraries: {count}")

            cursor.close()
            conn.close()
        except Exception as e:
            log_error(f"Error executing Hive query: {str(e)}")
            raise

        log_info("Silver layer data processed and table created as ORC")

        return SILVER_HDFS_PATH
    except Exception as e:
        log_error(f"Error in process_silver_layer: {str(e)}")
        raise AirflowException(f"Silver layer processing failed: {str(e)}")