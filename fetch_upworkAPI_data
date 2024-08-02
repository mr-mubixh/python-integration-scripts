import requests
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import logging
import os

# Set up logging
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

# Configuration
CLIENT_ID = 'dummy'
CLIENT_SECRET = 'dummy'
REFRESH_TOKEN = 'dummy'
# Im using file to store token so it will automatically update during the script runs
# TOKEN_FILE = 'access_token.txt'  # File to store the access token
TOKEN_FILE = '/root/upworkData/access_token.txt'  # server
GRAPHQL_API_URL = 'https://api.upwork.com/graphql'
TOKEN_URL = 'https://www.upwork.com/api/v3/oauth2/token'

MYSQL_HOST = 'dummy'
MYSQL_DATABASE = 'dummy'
MYSQL_USER = 'dummy'
MYSQL_PASSWORD = 'dummy'

# Define the GraphQL query with filter and pagination
query = '''
query contractTimeReport($filter: TimeReportFilter) {
  contractTimeReport(filter: $filter) {
    edges {
      node {
        dateWorkedOn
        weekWorkedOn
        monthWorkedOn
        yearWorkedOn
        freelancer {
          name
        }
        team {
          name
        }
        contract {
          status
        }
        termId
        task
        taskDescription
        memo
        totalHoursWorked
        totalOnlineHoursWorked
        totalOfflineHoursWorked
      }
    }
  }
}
'''


# Function to calculate the date range
def get_date_range():
    end_date = datetime.today()
    start_date = end_date - timedelta(days=2)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    # return '2024-07-25', '2024-07-30'


# Function to read the access token from file
def read_access_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'r') as file:
            return file.read().strip()
    return None


# Function to write the access token to file
def write_access_token(token):
    with open(TOKEN_FILE, 'w') as file:
        file.write(token)


# Function to refresh the access token
def refresh_access_token():
    global ACCESS_TOKEN
    logger.info("Refreshing access token...")
    try:
        response = requests.post(TOKEN_URL, data={
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'refresh_token': REFRESH_TOKEN,
            'grant_type': 'refresh_token'
        })
        response.raise_for_status()
        tokens = response.json()
        ACCESS_TOKEN = tokens['access_token']
        write_access_token(ACCESS_TOKEN)
        logger.info("Access token refreshed and saved successfully")
        return True
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while refreshing token: {http_err}")
    except Exception as err:
        logger.error(f"Error occurred while refreshing token: {err}")
    return False


# Fetch data from the GraphQL API
def fetch_data():
    global ACCESS_TOKEN

    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }

    start_date, end_date = get_date_range()
    # logger.info(f"Fetching data from Upwork API for the date range: {start_date} to {end_date}")

    filter_params = {
        "organizationId_eq": "dummy",
        "timeReportDate_bt": {
            "rangeStart": start_date,
            "rangeEnd": end_date
        }
    }

    variables = {
        'filter': filter_params
    }
    payload = {
        'query': query,
        'variables': variables
    }

    for attempt in range(5):  # Retry up to 5 times
        try:
            response = requests.post(GRAPHQL_API_URL, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            result = response.json()
            logger.info(f"Data fetched successfully from Upwork API: {start_date} to {end_date}")
            return result
        except requests.exceptions.HTTPError as http_err:
            if response.status_code == 401:  # Unauthorized, possibly expired token
                logger.error(f"Unauthorized error, attempting to refresh token: {http_err}")
                if refresh_access_token():
                    headers['Authorization'] = f'Bearer {ACCESS_TOKEN}'
                    continue  # Retry with new token
            logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
        if attempt == 4:
            logger.error("Failed to fetch data after multiple attempts.")
            break
    return None


# Store data in MySQL
def store_data_in_mysql(data):
    if data and 'data' in data and 'contractTimeReport' in data['data']:
        try:
            connection = mysql.connector.connect(
                host=MYSQL_HOST,
                database=MYSQL_DATABASE,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD
            )

            if connection.is_connected():
                cursor = connection.cursor()
                start_date, end_date = get_date_range()

                # Remove existing data for the date range
                # logger.info(f"Deleting existing data from database for the date range: {start_date} to {end_date}")
                delete_query = """
                DELETE FROM upwork_data WHERE date BETWEEN %s AND %s
                """
                cursor.execute(delete_query, (start_date, end_date))
                connection.commit()
                logger.info("Existing data deleted successfully")

                # Insert new data
                insert_query = """
                 INSERT INTO upwork_data (
                    date, week, month, year, talent, team_name, contract_status, 
                    term_id, task, task_description, memo, total_hours_worked, 
                    total_online_hours_worked, total_offline_hours_worked
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                edges = data['data']['contractTimeReport']['edges']
                values = [
                    (
                        edge['node']['dateWorkedOn'],
                        edge['node']['weekWorkedOn'],
                        edge['node']['monthWorkedOn'],
                        edge['node']['yearWorkedOn'],
                        edge['node']['freelancer']['name'],
                        edge['node']['team']['name'] if edge['node']['team'] else None,
                        edge['node']['contract']['status'] if edge['node']['contract'] else None,
                        edge['node']['termId'],
                        edge['node']['task'],
                        edge['node']['taskDescription'],
                        edge['node']['memo'],
                        edge['node']['totalHoursWorked'],
                        edge['node']['totalOnlineHoursWorked'],
                        edge['node']['totalOfflineHoursWorked'],
                    )
                    for edge in edges
                ]

                cursor.executemany(insert_query, values)
                connection.commit()
                logger.info("Data inserted successfully into the database")

        except Error as e:
            logger.error(f"Error while connecting to MySQL: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                logger.info("MySQL connection is closed")
    else:
        logger.warning("Data is missing or in unexpected format")


# Main function to execute the script
if __name__ == "__main__":
    logger.info("Script execution started")

    # Read the access token from file
    ACCESS_TOKEN = read_access_token() or ACCESS_TOKEN

    graphQL_data = fetch_data()
    if graphQL_data:
        store_data_in_mysql(graphQL_data)

    logger.info("Script execution finished")
