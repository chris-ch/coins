import logging
from datetime import datetime
import httplib2
from apiclient import discovery
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials

_GOOGLE_DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive'
_GOOGLE_DRIVE_FILE_SCOPE = 'https://www.googleapis.com/auth/drive.file'
_SHEET_TAB_PRICES = 'Prices'
_EPOCH_START = datetime(1899, 12, 31)


def file_by_id(svc_drive, file_id):
    response = svc_drive.files().get(fileId=file_id)
    return response.execute()


def authorize_services(credentials_file):
    """

    :param credentials_file:
    :return:
    """
    scopes = [_GOOGLE_DRIVE_SCOPE]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scopes=scopes)
    if not credentials or credentials.invalid:
        raise Exception('Invalid credentials')

    authorized_http = credentials.authorize(httplib2.Http())
    return authorized_http, credentials


def setup_services(credentials_file):
    """
    :param credentials_file: Google JSON Service Account credentials
    :return: tuple (Drive service, Sheets service)
    """
    authorized_http, credentials = authorize_services(credentials_file)
    svc_drive = discovery.build('drive', 'v3', http=authorized_http, cache_discovery=False)
    svc_sheets = discovery.build('sheets', 'v4', http=authorized_http, cache_discovery=False)
    return svc_drive, svc_sheets


def sheet_has_tab(svc_sheets, spreadsheet_id, tab_name):
    request_sheet = svc_sheets.spreadsheets().get(spreadsheetId=spreadsheet_id)
    sheet_properties = request_sheet.execute()

    for sheet_data in sheet_properties['sheets']:
        if sheet_data['properties']['title'] == tab_name:
            return True

    return False


def load_sheet(svc_sheets, spreadsheet_id):
    if not sheet_has_tab(svc_sheets, spreadsheet_id, _SHEET_TAB_PRICES):
        raise Exception('Google sheet {} does not have a tab "{}"'.format(spreadsheet_id, _SHEET_TAB_PRICES))

    request_portfolio = svc_sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=_SHEET_TAB_PRICES)
    response_portfolio = request_portfolio.execute()
    return response_portfolio


def update_sheet2(svc_sheet, spreadsheet_id, header, records):
    if len(records) == 0:
        return

    workbook = svc_sheet.open_by_key(spreadsheet_id)
    sheets = dict()
    for sheet in workbook.worksheets():
        sheets[sheet.title] = sheet

    worksheet = sheets[_SHEET_TAB_PRICES]

    count_columns = len(header)
    count_rows = len(records) + 1
    worksheet.resize(rows=count_rows, cols=count_columns)
    range_text = 'A1:{}'.format(rowcol_to_a1(count_rows, count_columns))
    logging.info('accessing range {}'.format(range_text))
    cells = worksheet.range(range_text)
    for cell in cells:
        count_row = cell.row - 1
        count_col = cell.col - 1
        field = header[count_col]
        if count_row == 0:
            cell.value = field

        else:
            row_data = records[count_row - 1]
            cell.value = row_data[field]

    worksheet.update_cells(cells)


def update_sheet(svc_sheets, spreadsheet_id, header, rows, date_columns=None, number_columns=None):
    """
    Updates the first available sheet from spreadsheet_id with the specified rows and header.

    :param svc_sheets: Google Sheets service
    :param spreadsheet_id:
    :param header:
    :param rows:
    :return:
    """
    if date_columns is None:
        date_columns = list()

    if number_columns is None:
        number_columns = list()

    spreadsheet_data = svc_sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    first_sheet_id = spreadsheet_data['sheets'][0]['properties']['sheetId']
    first_sheet_title = spreadsheet_data['sheets'][0]['properties']['title']
    clear_sheet_body = {
        'updateCells': {
            'range': {
                'sheetId': first_sheet_id
            },
            'fields': '*',
        }
    }
    header_row = [{'userEnteredValue': {'stringValue': header_field}} for header_field in header]
    set_sheet_properties_body = {
        'updateSheetProperties': {
            'properties': {
                'sheetId': first_sheet_id,
                'title': first_sheet_title,
                'index': 0,
                'gridProperties': {
                    'rowCount': len(rows) + 1,
                    'columnCount': len(header_row),
                    'frozenRowCount': 1,
                    'hideGridlines': False,
                },
            },
            'fields': '*',
        }
    }
    row_data = list()
    for row in rows[:3]:
        user_values = list()
        for field in header:
            extended_value = 'stringValue'
            field_value = row[field]
            if field in date_columns:
                extended_value = 'numberValue'
                days_since_start = row[field] - _EPOCH_START
                field_value = days_since_start.total_seconds() / 24 / 60 / 60

            elif field in number_columns:
                extended_value = 'numberValue'
                field_value = float(row[field])

            user_values.append({'userEnteredValue': {extended_value: field_value}})

        row_data.append(user_values)

    from pprint import pprint
    payload = {'values': [header_row] + row_data}
    pprint(payload)
    logging.info('writing {} columns and {} rows'.format(len(header), len(rows) + 1))
    cell_update_body = {
        'updateCells': {
            'range': {'sheetId': first_sheet_id,
                      'startRowIndex': 0, 'endRowIndex': len(rows) + 1,
                      'startColumnIndex': 0, 'endColumnIndex': len(header) + 30},
            'fields': '*',
            'rows': payload
        }
    }
    batch_update_body = {
        'requests': [clear_sheet_body, set_sheet_properties_body, cell_update_body]
    }
    svc_sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_body).execute()