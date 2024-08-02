import time
import logging
from google.cloud import bigquery
from dotenv import load_dotenv
import pendulum
import mysql.connector
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('process.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(message)s'))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(message)s'))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Load environment variables from the .env file
load_dotenv()
target_table = 'dummy'
transformed_table = 'dummy'
staging_table = 'dummy'


def get_timezones_from_mysql(account_names):
    try:
        mysql_host = os.getenv("MYSQL_HOST")
        mysql_user = os.getenv("MYSQL_USER")
        mysql_password = os.getenv("MYSQL_PASSWORD")
        mysql_database = os.getenv("MYSQL_DATABASE")

        connection = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )

        cursor = connection.cursor()
        format_strings = ','.join(['%s'] * len(account_names))
        query = f"""
            SELECT ad_account_code, timezone
            FROM mb_accountrelation_main
            WHERE ad_account_code IN ({format_strings})
        """
        cursor.execute(query, account_names)

        timezone_dict = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.close()
        connection.close()

        return timezone_dict

    except Exception as e:
        logger.error(f"Error fetching timezones from MySQL: {e}")
        return {}


def create_staging_table(client):
    schema = [
        bigquery.SchemaField("date_start", "DATE"),
        bigquery.SchemaField("date_stop", "DATE"),
        bigquery.SchemaField("account_currency", "STRING"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("account_name", "STRING"),
        bigquery.SchemaField("ad_set_id", "STRING"),
        bigquery.SchemaField("ad_set_name", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("amount_spend", "FLOAT64"),
        bigquery.SchemaField("hour", "STRING"),
        bigquery.SchemaField("source_datetime", "TIMESTAMP"),
        bigquery.SchemaField("timezone", "STRING"),
        bigquery.SchemaField("pacific_datetime", "TIMESTAMP"),
        bigquery.SchemaField("dimension__hourly_stats_aggregated_by_advertiser_time_zone", "STRING")
    ]
    table = bigquery.Table(staging_table, schema=schema)

    try:
        client.get_table(table)  # Check if table exists
        logger.info(f"Table {staging_table} already exists.")
    except:
        client.create_table(table)  # Create table if it doesn't exist
        logger.info(f"Created table {staging_table}.")


def insert_dummy_row_if_needed(client):
        dummy_row = [{
            "date_start": "1900-01-01",
            "date_stop": "1900-01-01",
            "account_currency": "DUMMY",
            "account_id": "DUMMY",
            "account_name": "DUMMY",
            "ad_set_id": "DUMMY",
            "ad_set_name": "DUMMY",
            "campaign_id": "DUMMY",
            "campaign_name": "DUMMY",
            "amount_spend": 0.0,
            "hour": "00:00:00",
            "source_datetime": pendulum.datetime(1900, 1, 1, 0, 0, 0, tz='UTC').to_iso8601_string(),
            "timezone": "UTC",
            "pacific_datetime": pendulum.datetime(1900, 1, 1, 0, 0, 0, tz='America/Los_Angeles').to_iso8601_string(),
            "dimension__hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:00:00"
        }]
        try:
            client.insert_rows_json(staging_table, dummy_row)
            logger.info("Inserted dummy row into staging table.")

        except Exception as e:
            logger.warning(f"Error inserting dummy row: {e}")


def query_bigquery():
    client = bigquery.Client()
    timezone_cache = {}

    query = f"""
           SELECT t.*,
                  SPLIT(t.Hourly_stats_aggregated_by_advertiser_time_zone, ' - ')[OFFSET(0)] as start_time,
                  SPLIT(t.Hourly_stats_aggregated_by_advertiser_time_zone, ' - ')[OFFSET(1)] as end_time
           FROM `{target_table}` t
           LEFT JOIN `{transformed_table}` tt
           ON CAST(t.Account_id AS STRING) = tt.account_id
           AND t.Date_start = tt.date_start
           AND SPLIT(t.Hourly_stats_aggregated_by_advertiser_time_zone, ' - ')[OFFSET(0)] = tt.hour
           WHERE tt.account_id IS NULL
       """

    try:
        query_job = client.query(query)
        results = query_job.result(page_size=1000)

        rows_to_insert = []

        for row in results:
            date_start = row['Date_start']
            date_stop = row['Date_stop']
            hourly_stats = row['Hourly_stats_aggregated_by_advertiser_time_zone']
            account_name = row['Account_name']
            account_id = row['Account_id']
            start_time, end_time = hourly_stats.split(' - ')

            if account_name in timezone_cache:
                timezone = timezone_cache[account_name]
            else:
                timezone_dict = get_timezones_from_mysql([account_name])
                timezone = timezone_dict.get(account_name, None)
                timezone_cache[account_name] = timezone

            use_default_date = timezone is None
            timezone = timezone or 'America/Los_Angeles'

            start_hour, start_minute, start_second = map(int, start_time.split(':'))
            end_hour, end_minute, end_second = map(int, end_time.split(':'))

            if use_default_date:
                start_time_fetched = pendulum.datetime(1900, 1, 1, start_hour, start_minute, start_second,
                                                       tz='America/Los_Angeles')
            else:
                start_time_fetched = pendulum.datetime(date_start.year, date_start.month, date_start.day, start_hour,
                                                       start_minute, start_second, tz=timezone)

            start_time_pacific = start_time_fetched.in_timezone('America/Los_Angeles')
            formatted_start_time = start_time_pacific.strftime('%H:%M:%S')

            if use_default_date:
                end_time_fetched = pendulum.datetime(1900, 1, 1, end_hour, end_minute, end_second,
                                                     tz='America/Los_Angeles')
            else:
                end_time_fetched = pendulum.datetime(date_start.year, date_start.month, date_start.day, end_hour,
                                                     end_minute, end_second, tz=timezone)

            end_time_pacific = end_time_fetched.in_timezone('America/Los_Angeles')
            formatted_end_time = end_time_pacific.strftime('%H:%M:%S')

            source_datetime = start_time_fetched.to_iso8601_string()
            pacific_datetime = start_time_pacific.to_iso8601_string()

            date_start_new = '1900-01-01' if use_default_date else date_start.strftime('%Y-%m-%d')
            if use_default_date:
                pacific_datetime = pendulum.datetime(1900, 1, 1, start_hour, start_minute, start_second,
                                                     tz='America/Los_Angeles').to_iso8601_string()

            row_to_insert = {
                "date_start": date_start.strftime('%Y-%m-%d'),
                "date_stop": date_stop.strftime('%Y-%m-%d'),
                "account_currency": row['Account_currency'],
                "account_id": account_id,
                "account_name": account_name,
                "ad_set_id": row['Ad_set_id'],
                "ad_set_name": row['Ad_set_name'],
                "campaign_id": row['Campaign_id'],
                "campaign_name": row['Campaign_name'],
                "amount_spend": row['Amount_spend'],
                "hour": formatted_start_time,
                "source_datetime": source_datetime,
                "timezone": timezone,
                "pacific_datetime": pacific_datetime,
                "dimension__hourly_stats_aggregated_by_advertiser_time_zone": f"{formatted_start_time} - {formatted_end_time}"
            }

            rows_to_insert.append(row_to_insert)

            if len(rows_to_insert) >= 10000:  # Adjust batch size as necessary
                load_data_to_staging(client, rows_to_insert)
                rows_to_insert = []

        if rows_to_insert:
            load_data_to_staging(client, rows_to_insert)

    except Exception as e:
        logger.error(f"Error querying BigQuery: {e}")


def load_data_to_staging(client, rows_to_insert):
    try:
        # Convert datetime fields to ISO 8601 format for TIMESTAMP
        for row in rows_to_insert:
            row["source_datetime"] = pendulum.parse(row["source_datetime"]).to_iso8601_string()
            row["pacific_datetime"] = pendulum.parse(row["pacific_datetime"]).to_iso8601_string()

        # Insert data into the staging table
        errors = client.insert_rows_json(staging_table, rows_to_insert)

        if errors:
            logger.error(f"Errors occurred while inserting rows into staging table: {errors}")
            return

        logger.info(f"{len(rows_to_insert)} rows inserted into staging table.")

    except Exception as e:
        logger.error(f"Error loading data into staging table: {e}")


def merge_staging_to_transformed(client):
    try:
        merge_query = f"""
            MERGE `{transformed_table}` AS target
            USING `{staging_table}` AS source
            ON target.account_id = source.account_id
            AND target.date_start = source.date_start
            AND target.hour = source.hour
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (date_start, date_stop, account_currency, account_id, account_name, ad_set_id, ad_set_name, campaign_id, campaign_name, amount_spend, hour, source_datetime, timezone, pacific_datetime, dimension__hourly_stats_aggregated_by_advertiser_time_zone)
                VALUES (source.date_start, source.date_stop, source.account_currency, source.account_id, source.account_name, source.ad_set_id, source.ad_set_name, source.campaign_id, source.campaign_name, source.amount_spend, source.hour, source.source_datetime, source.timezone, source.pacific_datetime, source.dimension__hourly_stats_aggregated_by_advertiser_time_zone)
        """

        merge_job = client.query(merge_query)
        merge_job.result()  # Wait for the job to complete

        logger.info(f"Data merged from staging table to transformed table.")
    except Exception as e:
        logger.error(f"Error merging data from staging table to transformed table: {e}")


def empty_staging_table():
    try:
        client = bigquery.Client()

        query = f"TRUNCATE TABLE `{staging_table}`"

        query_job = client.query(query)

        try:
            query_job.result()
            logging.info("Table has been emptied.")
        except Exception as e:
            logging.error(f"An error occurred while emptying the table: {e}")

    except Exception as e:
        logging.error(f"Error emptying table: {e}")


if __name__ == "__main__":
    load_dotenv()
    start_time = time.time()
    logger.info(f"###############################{target_table}  Script started. ##################### {start_time}")
    client = bigquery.Client()
    create_staging_table(client)
    insert_dummy_row_if_needed(client)
    query_bigquery()
    merge_staging_to_transformed(client)
    empty_staging_table()
    end_time = time.time()
    logger.info(f"{target_table} Script finished. Execution time: {end_time - start_time} seconds")
