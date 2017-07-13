import logging

import httplib2
from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

_GOOGLE_DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive'
_GOOGLE_DRIVE_FILE_SCOPE = 'https://www.googleapis.com/auth/drive.file'


def file_by_id(svc_drive, file_id):
    response = svc_drive.files().get(fileId=file_id)
    return response.execute()


def setup_services(credentials_file):
    """
    :param credentials_file: Google JSON Service Account credentials
    :return: tuple (Drive service, Sheets service)
    """
    scopes = [_GOOGLE_DRIVE_SCOPE]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scopes=scopes)
    if not credentials or credentials.invalid:
        raise Exception('Invalid credentials')

    authorized_http = credentials.authorize(httplib2.Http())
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
    if not sheet_has_tab(svc_sheets, spreadsheet_id, 'Portfolio'):
        raise Exception('Google sheet {} does not have a tab "{}"'.format(spreadsheet_id, 'Portfolio'))

    request_portfolio = svc_sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range='Portfolio')
    response_portfolio = request_portfolio.execute()
    return response_portfolio


def update_sheet(svc_sheets, spreadsheet_id, header, rows):
    """
    Updates the first available sheet from spreadsheet_id with the specified rows and header.

    :param svc_sheets: Google Sheets service
    :param spreadsheet_id:
    :param header:
    :param rows:
    :return:
    """
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
    set_sheet_properties_body = {
        'updateSheetProperties': {
            'properties': {
                'sheetId': first_sheet_id,
                'title': first_sheet_title,
                'index': 0,
                'gridProperties': {
                    'rowCount': len(rows) + 1,
                    'columnCount': 10,
                    'frozenRowCount': 1,
                    'hideGridlines': False,
                },
            },
            'fields': '*',
        }
    }
    cell_update_body = {
        'updateCells': {
            'range': {'sheetId': first_sheet_id,
                      'startRowIndex': 0, 'endRowIndex': len(rows) + 1,
                      'startColumnIndex': 0, 'endColumnIndex': 4},
            'fields': '*',
            'rows': [{'values': [
                {'userEnteredValue': {'stringValue': header_field}} for header_field in header]}] + [
                        {'values': [{'userEnteredValue': {'stringValue': row[field]}} for field in header]}
                        for row in rows]
        }
    }
    batch_update_body = {
        'requests': [clear_sheet_body, set_sheet_properties_body, cell_update_body]
    }
    svc_sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_body).execute()