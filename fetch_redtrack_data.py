import requests
import mysql.connector
from datetime import datetime, timedelta
import argparse
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('redtrack_data.log')  # Log to a file
    ]
)

# RedTrack API details
API_KEY = 'dummy'  # Ensure this is the correct API key
API_URL = 'dummy'  # Replace with the correct endpoint

# MySQL Centos database details
MYSQL_HOST = 'dummy'
MYSQL_DB = 'dummy'
MYSQL_USER = 'dummy'
MYSQL_PASSWORD = 'dummy'


def fetch_redtrack_data(from_date, to_date):
    headers = {
        'accept': 'application/json'
    }
    params = {
        'api_key': API_KEY,
        'group': 'campaign',
        'date_from': from_date,
        'date_to': to_date
    }

    try:
        response = requests.get(API_URL, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error: Request failed with status code {response.status_code}")

    except Exception as e:
        logging.error(f"Error occurred: {e}")

    return []


def insert_data_into_mysql(connection, data, date):
    cursor = connection.cursor()
    try:
        # Check if data for the given date already exists
        check_sql = "SELECT COUNT(*) FROM campaign_metrics WHERE date = %s"
        cursor.execute(check_sql, (date,))
        if cursor.fetchone()[0] > 0:
            logging.info(f"Data for date {date} already exists in the database. Skipping...")
            return

        logging.info(f'Data found for date {current_date_str}')
        total_inserted_rows = 0

        for record in data:
            campaign_name = record.get('campaign', 'N/A')
            revenue = record.get('total_revenue', 0)
            cost = record.get('cost', 0)

            sql = (f"INSERT INTO campaign_metrics (date, campaign_name, revenue, cost) "
                   f" VALUES (%s, %s, %s, %s)")
            cursor.execute(sql, (date, campaign_name, revenue, cost))
            total_inserted_rows += 1

        connection.commit()
        logging.info(f"{total_inserted_rows} records inserted successfully into MySQL for date {date}")

    except mysql.connector.Error as e:
        logging.error(f"Error inserting data into MySQL: {e}")

    finally:
        cursor.close()


if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Fetch and store RedTrack data.')
    parser.add_argument('start_date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('end_date', type=str, help='End date in YYYY-MM-DD format')

    args = parser.parse_args()

    start_date_dt = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(args.end_date, '%Y-%m-%d')
    current_date_dt = start_date_dt

    # Establish the MySQL connection once
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        port=3306,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

    while current_date_dt <= end_date_dt:
        current_date_str = current_date_dt.strftime('%Y-%m-%d')

        retries = 3
        while retries > 0:
            redtrack_data = fetch_redtrack_data(current_date_str, current_date_str)
            if redtrack_data:
                insert_data_into_mysql(connection, redtrack_data, current_date_str)
                break
            else:
                logging.warning(f'No data found for date {current_date_str}. Retrying...')
                retries -= 1
                time.sleep(5)  # Wait for 5 seconds before retrying

        if retries == 0:
            logging.error(f'Failed to fetch data for date {current_date_str} after 3 attempts.')

        current_date_dt += timedelta(days=1)
        time.sleep(2)  # Delay to prevent hitting rate limits

    # Close the connection once at the end
    if connection.is_connected():
        connection.close()
        logging.info("MySQL connection is closed")
