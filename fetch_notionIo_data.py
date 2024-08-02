import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import sys
import time
from gspread_formatting import CellFormat, Color, TextFormat, format_cell_range

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('notion_data.log')  # Log to a file
    ]
)

# Notion API setup
NOTION_API_KEY = 'dummy'
NOTION_DATABASE_ID = 'dummy'
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}


def fetch_notion_data_new(database_id):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    all_data = []
    has_more = True
    next_cursor = None

    while has_more:
        payload = {}
        if next_cursor:
            payload['start_cursor'] = next_cursor

        response = requests.post(url, headers=NOTION_HEADERS, json=payload)
        data = response.json()
        all_data.extend(data.get('results', []))

        next_cursor = data.get('next_cursor')
        has_more = data.get('has_more')

    return {'results': all_data}


# */30 * * * * /usr/bin/python /root/notionIo/fetch_notion_data.py >> /root/notionIo/notion_to_sheets.log 2>&1


# Google Sheets API setup
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name('dummy', scope)
client = gspread.authorize(creds)

# Open the Google Sheet
spreadsheet = client.open("Notion_Data")
sheet = spreadsheet.sheet1  # Get the first sheet


# Function to update Google Sheet with Notion data
def update_google_sheet(data):
    rows = []
    headings = []
    row_number = 1

    # Extract headings from the first result
    first_result = data.get('results', [{}])[0]
    properties = first_result.get('properties', {})
    # Clear the sheet before updating
    sheet.clear()
    for prop, value in properties.items():
        headings.append(prop)  # Use property names as headings

    # Append headings as the first row
    sheet.append_row(headings)

    # Format the heading row
    format_cell_range(sheet, '1:1', CellFormat(
        backgroundColor=Color(0.678, 0.847, 0.902),
        textFormat=TextFormat(bold=True),
        horizontalAlignment='CENTER'
    ))

    # Append data rows
    for result in data.get('results', []):
        row = []
        prop_number = 1
        properties = result.get('properties', {})
        for prop, value in properties.items():
            # if row_number == 2:
            #     logging.error(f' prop : {prop}')
            #     logging.error(f' value : {value}')
            # logging.info(f'################## : {prop_number} : ##################')
            prop_number = prop_number + 1
            value_type = value.get('type')

            if value_type == 'title':
                title_content = value.get('title', [])
                if title_content:
                    row.append(title_content[0].get('plain_text', ''))
                else:
                    row.append('')
            elif value_type == 'rich_text':
                rich_text_content = value.get('rich_text', [])
                if rich_text_content:
                    row.append(rich_text_content[0].get('plain_text', ''))
                else:
                    row.append('')
            elif value_type == 'multi_select':
                multi_select_content = value.get('multi_select', [])
                row.append(', '.join([item.get('name', '') for item in multi_select_content]))
            elif value_type == 'date':
                date_content = value.get('date', {})
                if date_content:
                    row.append(date_content.get('start', ''))
                else:
                    row.append('')
            elif value_type == 'relation':
                relation_content = value.get('relation', [])
                row.append(', '.join([item.get('id', '') for item in relation_content]))
            elif value_type == 'rollup':
                rollup_content = value.get('rollup', {})
                rollup_array = rollup_content.get('array', [])
                if rollup_array:
                    row.append(', '.join([item.get('name', '') for item in rollup_array]))
                else:
                    row.append('')
            elif value_type == 'checkbox':
                row.append(str(value.get('checkbox', False)))
            elif value_type == 'files':
                files_content = value.get('files', [])
                row.append(', '.join([file.get('name', '') for file in files_content]))
            elif value_type in ['number', 'url', 'email', 'phone_number']:
                row.append(value.get(value_type, ''))
            elif value_type in ['select', 'status']:
                content = value.get(value_type, {})
                if content:
                    row.append(content.get('name', ''))
                else:
                    row.append('')
            else:
                row.append(value.get('id', ''))

        # Swap the first and last column in the row
        if row:
            row[0], row[-1] = row[-1], row[0]

        rows.append(row)
        row_number += 1
    logging.info(f'Rows to be updated in Google Sheet: {len(rows)}')

    # Pad rows with empty strings
    for row in rows:
        while len(row) < len(headings):
            row.append('')

    # Insert the data into the sheet
    sheet.update(rows, 'A2')  # Batch update

if __name__ == "__main__":
    notion_data = fetch_notion_data_new(NOTION_DATABASE_ID)
    update_google_sheet(notion_data)
