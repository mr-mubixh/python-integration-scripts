# README (1- Upwork API Integration)
## Overview

This script is designed to fetch data from the Upwork GraphQL API, process it, and store it in a MySQL database. It includes functionality for handling access tokens, fetching and storing data, logging, and retry mechanisms to ensure reliable execution.

## Prerequisites

Before running the script, ensure the following are installed and configured:

1. **Python 3.6+**
2. **Python Libraries:**
   - `requests`
   - `mysql-connector-python`
3. **MySQL Database:**
   - A MySQL database should be available and accessible with the necessary permissions.

## Setup

### Configuration

Modify the configuration values in the script to match your environment:

- **Upwork API Configuration:**
  - `CLIENT_ID`: Your Upwork API client ID.
  - `CLIENT_SECRET`: Your Upwork API client secret.
  - `REFRESH_TOKEN`: The refresh token to obtain new access tokens.
  - `TOKEN_FILE`: Path to the file where the access token will be stored.
  - `GRAPHQL_API_URL`: The Upwork GraphQL API endpoint.
  - `TOKEN_URL`: The Upwork API token URL.

- **MySQL Configuration:**
  - `MYSQL_HOST`: The hostname or IP address of your MySQL server.
  - `MYSQL_DATABASE`: The name of the database where data will be stored.
  - `MYSQL_USER`: The MySQL username with access to the database.
  - `MYSQL_PASSWORD`: The password for the MySQL user.

### Logging

Logs are stored in a file named `process.log` in the current directory. The script logs to both the console and the log file, capturing key events and errors during execution.

## Usage

### Running the Script

1. **Ensure the access token is available:**
   - The script reads the access token from a file specified in `TOKEN_FILE`. If the token is not found or has expired, it attempts to refresh it using the `REFRESH_TOKEN`.

2. **Execute the script:**
   ```bash
   python script_name.py
   ```
   Replace `script_name.py` with the actual filename.

### Script Workflow

1. **Date Range Calculation:**
   - The script calculates a date range (last 2 days by default) to fetch data from the Upwork API.

2. **Data Fetching:**
   - Sends a GraphQL query to the Upwork API to retrieve contract time report data.

3. **Data Storage:**
   - Connects to the MySQL database and deletes existing data within the specified date range.
   - Inserts the newly fetched data into the database.

### Error Handling and Retries

- The script includes retry mechanisms for API requests, particularly for handling access token expiration and other network-related issues.
- If the script fails after multiple attempts, errors are logged for further investigation.

## Troubleshooting

- **Connection Errors:**
  - Ensure the MySQL database is accessible and the credentials are correct.
  
- **Token Errors:**
  - Verify the `CLIENT_ID`, `CLIENT_SECRET`, and `REFRESH_TOKEN` values are correct.
  - Ensure the `TOKEN_FILE` path is correct and writable.

- **Logging:**
  - Check the `process.log` file for detailed logs of the script execution.



# README (2- BigQuery data Integration)
## Overview

This script processes data from Google BigQuery, performs transformations, and inserts the results into a MySQL database. It includes functionality for handling time zones, managing staging and transformed tables in BigQuery, and logging the entire process.

## Prerequisites

Before running the script, ensure the following are installed and configured:

1. **Python 3.6+**
2. **Google Cloud SDK** with BigQuery API enabled.
3. **Python Libraries:**
   - `google-cloud-bigquery`
   - `mysql-connector-python`
   - `python-dotenv`
   - `pendulum`
4. **MySQL Database:**
   - A MySQL database should be available and accessible with the necessary permissions.
5. **.env File:** 
   - Create a `.env` file in the root directory containing the MySQL configuration.

### Example `.env` file:
```ini
MYSQL_HOST=your_mysql_host
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=your_mysql_database
```

## Setup

### Configuration

Modify the configuration values in the script to match your environment:

- **Google BigQuery Configuration:**
  - `target_table`: The source table in BigQuery.
  - `transformed_table`: The destination table in BigQuery for transformed data.
  - `staging_table`: A staging table in BigQuery used for interim data storage.

- **MySQL Configuration:**
  - These are stored in the `.env` file as shown above.

### Logging

Logs are stored in a file named `process.log` in the current directory. The script logs to both the console and the log file, capturing key events and errors during execution.

## Usage

### Running the Script

1. **Ensure environment variables are loaded:**
   - The script uses `python-dotenv` to load MySQL credentials from a `.env` file.

2. **Execute the script:**
   ```bash
   python script_name.py
   ```
   Replace `script_name.py` with the actual filename.

### Script Workflow

1. **Staging Table Creation:**
   - Creates a staging table in BigQuery if it doesn't already exist.

2. **Query BigQuery:**
   - Retrieves data from the target table, processes time zone information, and prepares the data for insertion.

3. **Insert Data into Staging Table:**
   - Inserts the processed data into the staging table, with batching to handle large datasets.

4. **Merge Staging to Transformed Table:**
   - Merges data from the staging table into the transformed table based on matching criteria.

5. **Empty Staging Table:**
   - Clears the staging table after successful data insertion.

### Error Handling and Logging

- The script includes error handling for database connections, BigQuery operations, and data processing. 
- Errors are logged to the `process.log` file, and critical errors are displayed in the console.

## Troubleshooting

- **Connection Errors:**
  - Ensure the BigQuery and MySQL credentials are correctly configured and accessible.
  
- **BigQuery Errors:**
  - Verify that the necessary tables exist in BigQuery and the appropriate permissions are set.

- **Logging:**
  - Check the `process.log` file for detailed logs of the script execution.


# README (3- NotionIo Data)

## Overview

This script fetches data from a Notion database using the Notion API and updates a Google Sheet with the retrieved data. The script is designed to run periodically, updating the Google Sheet with the latest Notion data, including formatting for better readability.

## Prerequisites

Before running the script, ensure the following are installed and configured:

1. **Python 3.6+**
2. **Google Cloud SDK** for authentication and access to Google Sheets.
3. **Python Libraries:**
   - `requests`
   - `gspread`
   - `oauth2client`
   - `gspread_formatting`
4. **Google Service Account:**
   - Create a Google Cloud Service Account and download the credentials JSON file.
   - Share the target Google Sheet with the service account email.
5. **Notion API Token:**
   - Obtain a Notion API key and have the database ID ready for accessing the data.

### Example `.env` file:
```ini
NOTION_API_KEY=your_notion_api_key
NOTION_DATABASE_ID=your_notion_database_id
```

## Setup

### Configuration

Modify the configuration values in the script to match your environment:

- **Google Sheets Configuration:**
  - The script uses a Service Account JSON file for Google Sheets API authentication.
  - Update the path to your JSON keyfile in `creds = ServiceAccountCredentials.from_json_keyfile_name()`.

- **Notion API Configuration:**
  - Update the `NOTION_API_KEY` and `NOTION_DATABASE_ID` variables with your Notion credentials.

### Logging

Logs are stored in a file named `notion_data.log` in the current directory. The script logs to both the console and the log file, capturing key events and errors during execution.

## Usage

### Running the Script

1. **Ensure environment variables are loaded:**
   - Configure your Notion API key and Database ID in the script or via a `.env` file.

2. **Execute the script:**
   ```bash
   python script_name.py
   ```
   Replace `script_name.py` with the actual filename.

### Script Workflow

1. **Fetch Data from Notion:**
   - The script queries the Notion API to fetch data from the specified database.

2. **Update Google Sheet:**
   - Clears the Google Sheet and inserts the fetched data, with formatted headings and cells for clarity.

3. **Error Handling:**
   - The script includes error handling for HTTP requests, data parsing, and Google Sheets API interactions.

### Scheduling

You can set up a cron job to run the script at regular intervals. Example crontab entry to run the script every 30 minutes:
```bash
*/30 * * * * /usr/bin/python /path/to/fetch_notion_data.py >> /path/to/notion_to_sheets.log 2>&1
```

### Error Handling and Logging

- Errors encountered during the process are logged to the `notion_data.log` file.
- The script also prints logs to the console for real-time monitoring.

## Troubleshooting

- **Connection Errors:**
  - Ensure your Notion API key is valid and that the Notion database ID is correct.
  
- **Google Sheets Errors:**
  - Verify that the service account has access to the Google Sheet.

- **Logging:**
  - Check the `notion_data.log` file for detailed logs of the script execution.

# README (4- Redtrack Data)

## Overview

This script fetches campaign data from the RedTrack API for a specified date range and inserts the data into a MySQL database. It is designed to handle large data requests over multiple days by iterating over the specified date range, fetching data for each day, and storing it in the database.

## Prerequisites

Before running the script, ensure that you have the following installed and configured:

1. **Python 3.6+**
2. **Python Libraries:**
   - `requests`
   - `mysql-connector-python` (or `mysql-connector`)
3. **MySQL Server:**
   - Ensure MySQL is installed and accessible from your script.
   - Create the necessary database and table (`campaign_metrics`) for storing the data.

### Example `.env` file:
```ini
API_KEY=your_redtrack_api_key
MYSQL_HOST=your_mysql_host
MYSQL_DB=your_mysql_database
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password
```

## Setup

### Configuration

Modify the configuration values in the script to match your environment:

- **RedTrack API Configuration:**
  - Update the `API_KEY` with your RedTrack API key.
  - Update the `API_URL` if the endpoint changes.

- **MySQL Configuration:**
  - Update the `MYSQL_HOST`, `MYSQL_DB`, `MYSQL_USER`, and `MYSQL_PASSWORD` variables with your MySQL connection details.

### MySQL Database Setup

Create the necessary table in your MySQL database. Example SQL statement:
```sql
CREATE TABLE campaign_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    campaign_name VARCHAR(255) NOT NULL,
    revenue DECIMAL(10, 2) NOT NULL,
    cost DECIMAL(10, 2) NOT NULL
);
```

### Logging

Logs are stored in a file named `redtrack_data.log` in the current directory. The script logs to both the console and the log file, capturing key events and errors during execution.

## Usage

### Running the Script

1. **Ensure environment variables are loaded:**
   - Configure your RedTrack API key and MySQL connection details in the script or via a `.env` file.

2. **Execute the script:**
   ```bash
   python script_name.py YYYY-MM-DD YYYY-MM-DD
   ```
   Replace `script_name.py` with the actual filename and provide the start and end dates in `YYYY-MM-DD` format.

### Script Workflow

1. **Fetch Data from RedTrack:**
   - The script queries the RedTrack API for campaign data between the specified start and end dates.
   - Data is fetched day by day to handle large date ranges.

2. **Insert Data into MySQL:**
   - For each day's data, the script checks if the data already exists in the database.
   - If data for the day does not exist, it inserts the data into the `campaign_metrics` table.

3. **Error Handling:**
   - The script retries fetching data up to 3 times for each day if the API call fails or no data is returned.
   - Errors encountered during MySQL interactions or API requests are logged for troubleshooting.

### Scheduling

You can set up a cron job to run the script at regular intervals. Example crontab entry to run the script daily:
```bash
0 1 * * * /usr/bin/python /path/to/script_name.py $(date +\%Y-\%m-\%d) $(date +\%Y-\%m-\%d) >> /path/to/redtrack_data.log 2>&1
```

### Error Handling and Logging

- **API Errors:**
  - The script retries failed API requests up to 3 times with a delay of 5 seconds between attempts.
  
- **MySQL Errors:**
  - Verify that the MySQL connection details are correct and that the database and table exist.

- **Logging:**
  - Check the `redtrack_data.log` file for detailed logs of the script execution.

## Troubleshooting

- **Connection Errors:**
  - Ensure your RedTrack API key is valid and that your MySQL credentials are correct.
  
- **Data Not Inserting:**
  - Check the log file to see if data for a given date already exists or if there were errors during the insert operation.


#### License

All scripts are proprietary and intended for internal use only. Redistribution or modification without permission is prohibited.

---

For any issues or questions, contact the development team.
