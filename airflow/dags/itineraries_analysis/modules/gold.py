from airflow.exceptions import AirflowException
from itineraries_analysis.utils import log_info, log_error
from itineraries_analysis.config import GOLD_HDFS_PATH
from pyhive import hive

def display_table_data(cursor, table_name):
    log_info(f"Displaying data for {table_name}:")
    
    # Get column names
    cursor.execute(f"DESCRIBE {table_name}")
    columns = [col[0] for col in cursor.fetchall()]
    log_info(f"Columns: {', '.join(columns)}")
    
    # Get and display sample data
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10")
    results = cursor.fetchall()
    for row in results:
        log_info(f"  {row}")
    
    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    log_info(f"Total rows in {table_name}: {count}")

def process_gold_layer(**kwargs):
    try:
        queries = [
            (f"""
            DROP TABLE IF EXISTS gold_avg_price_by_airline;
            CREATE TABLE gold_avg_price_by_airline (
                airline_code STRING,
                airline_name STRING,
                avg_price DOUBLE,
                flight_count BIGINT
            )
            STORED AS PARQUET
            LOCATION '{GOLD_HDFS_PATH}/avg_price_by_airline';
            
            INSERT INTO TABLE gold_avg_price_by_airline
            SELECT 
                segmentsAirlineCode as airline_code,
                segmentsAirlineName as airline_name,
                AVG(totalFare) as avg_price,
                COUNT(*) as flight_count
            FROM silver_itineraries
            GROUP BY segmentsAirlineCode, segmentsAirlineName
            """, "gold_avg_price_by_airline"),
            
            (f"""
            DROP TABLE IF EXISTS gold_frequent_routes;
            CREATE TABLE gold_frequent_routes (
                starting_airport STRING,
                destination_airport STRING,
                frequency BIGINT
            )
            STORED AS PARQUET
            LOCATION '{GOLD_HDFS_PATH}/frequent_routes';
            
            INSERT INTO TABLE gold_frequent_routes
            SELECT 
                startingAirport as starting_airport,
                destinationAirport as destination_airport,
                COUNT(*) as frequency
            FROM silver_itineraries
            GROUP BY startingAirport, destinationAirport
            ORDER BY frequency DESC
            LIMIT 100
            """, "gold_frequent_routes"),
            
            (f"""
            DROP TABLE IF EXISTS gold_price_distribution_by_month;
            CREATE TABLE gold_price_distribution_by_month (
                month INT,
                min_price DOUBLE,
                max_price DOUBLE,
                avg_price DOUBLE,
                median_price DOUBLE,
                flight_count BIGINT
            )
            STORED AS PARQUET
            LOCATION '{GOLD_HDFS_PATH}/price_distribution_by_month';
            
            INSERT INTO TABLE gold_price_distribution_by_month
            SELECT 
                MONTH(flight_date) as month,
                MIN(totalFare) as min_price,
                MAX(totalFare) as max_price,
                AVG(totalFare) as avg_price,
                percentile_approx(cast(totalFare as double), 0.5) as median_price,
                COUNT(*) as flight_count
            FROM silver_itineraries
            GROUP BY MONTH(flight_date)
            ORDER BY month
            """, "gold_price_distribution_by_month")
        ]

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
            
            for query, table_name in queries:
                log_info(f"Executing Hive query to create/update {table_name}")
                for statement in query.split(';'):
                    if statement.strip():
                        cursor.execute(statement.strip())
                        log_info(f"Executed statement: {statement.strip()}")
                log_info(f"Hive query executed successfully for {table_name}")
            
            # Display data for all created tables
            tables_to_display = ['gold_avg_price_by_airline', 'gold_frequent_routes', 'gold_price_distribution_by_month']
            
            for table in tables_to_display:
                display_table_data(cursor, table)
            
            cursor.close()
            conn.close()
        except Exception as e:
            log_error(f"Error executing Hive query: {str(e)}")
            raise

        log_info("Gold layer tables created/updated and displayed successfully")

        return GOLD_HDFS_PATH
    except Exception as e:
        log_error(f"Error in process_gold_layer: {str(e)}")
        raise AirflowException(f"Gold layer processing failed: {str(e)}")