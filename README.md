# README
1- Upwork API Integration
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

## License

This script is proprietary and intended for internal use only. Redistribution or modification without permission is prohibited.

---

For any issues or questions, contact the development team.
