import logging
import httplib2
from apiclient import discovery
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials

_GOOGLE_DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive'
_GOOGLE_DRIVE_FILE_SCOPE = 'https://www.googleapis.com/auth/drive.file'
_SHEET_TAB_PRICES = 'Prices'


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


def update_sheet(svc_sheet, spreadsheet_id, header, records):
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
