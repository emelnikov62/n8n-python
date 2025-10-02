from datetime import datetime

import psycopg2
from flask import Flask
from flask import request
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pystreamapi import Stream

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file",
          "https://www.googleapis.com/auth/spreadsheets"]
TOKEN_FILE = "token.json"
CREDS_FILE = "creds.json"
FAIL = 'fail'
NOT_FOUND = 'not_found'
SUCCESS = 'success'
INSERT = 'insert'
DELETE = 'delete'

API_PG = 'postgres'
API_EXCEL = 'google sheets'
API_REST = 'rest'
app = Flask(__name__)


# Add record to pg
def addRecordPg(cursor, data, params):
    columns = str(Stream.of(params[15]).map(lambda x: x['key']).to_tuple())
    values = Stream.of(data.get('fields')).map(lambda x: data.get('fields').get(x)).to_tuple()

    sql = 'insert into {0}.{1} {2} values {3} returning id'.format(params[8], params[9], columns.replace("'", ""),
                                                                   values)
    cursor.execute(sql)
    idRecord = cursor.fetchone()[0]
    if idRecord is None:
        return {'status': "fail", 'message': 'error add record in pg', 'type': API_PG}

    return {'status': "success", 'type': API_PG}


# Cancel record from pg
def cancelRecordPg(cursor, data, params):
    values = Stream.of(data.get('fields')).map(
        lambda x: "{0}::text = '{1}'::text".format(x.replace("'", ""), data.get('fields').get(x))).to_tuple()
    print(' and '.join(values))

    sql = 'delete from {0}.{1} where {2}'.format(params[8], params[9], ' and '.join(values))
    print(sql)
    cursor.execute(sql)

    if cursor.rowcount != 0:
        return {'status': "success", 'type': API_PG}
    else:
        return {'status': NOT_FOUND, 'message': 'Извините, не удалось найти Вашу запись! Вы хотите записаться?',
                'type': API_PG}


# Get Excel API creds
def getExcelCreds():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return creds


# Add record in excel
def addRecordExcel(service, spreadsheet_id, value_input_option, range_name, data):
    values = [Stream.of(data.get('fields')).map(lambda x: data.get('fields').get(x)).to_list()]
    body = {"values": values}
    rows = (
        service
        .spreadsheets()
        .values()
        .update(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption=value_input_option, body=body)
        .execute()
    )

    if rows.get('updatedCells') is None:
        return {'status': "fail", 'message': 'error add excel record', 'type': API_EXCEL}

    return {'status': "success", 'type': API_EXCEL}


# Cancel record in excel
def cancelRecordExcel(service, spreadsheet_id, data):
    rows = (service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range='Data!A:Z').execute())
    valuesExcel = rows.get("values", [])
    if valuesExcel is None:
        return {'status': "fail", 'message': 'error cancel excel record', 'type': API_EXCEL}

    found = 0
    values = Stream.of(data.get('fields')).map(lambda x: data.get('fields').get(x)).to_list()

    max = len(values)
    for index, val in enumerate(valuesExcel, start=1):
        for i in range(0, max - 1):
            if val[i].strip() == values[i].strip():
                found = index
                break

    if found != 0:
        rows = (
            service
            .spreadsheets()
            .values()
            .clear(spreadsheetId=spreadsheet_id, range=f"Data!A{found}:Z{found}")
            .execute()
        )

        if rows is None:
            return {'status': "fail", 'message': 'error cancel excel record', 'type': API_EXCEL}
        else:
            return {'status': "success", 'type': API_EXCEL}

    return {'status': NOT_FOUND, 'message': 'Извините, не удалось найти Вашу запись! Вы хотите записаться?',
            'type': API_EXCEL}


# Post rest webhook
@app.post('/api/integration')
def webhookIntegration():
    # data = {
    #     'fields': {
    #         'surname': 'Иванов',
    #         'name': 'Иван',
    #         'phone': '89009009090',
    #         'doctor': 'окулист',
    #         'date_time': '2025-09-29 11:59:39.019769'
    #     },
    #     'database': 'n8n_db',
    #     'user': 'n8n_user',
    #     'password': 'Mery1029384756$',
    #     'host': 'n8n-db-emelnikov62.db-msk0.amvera.tech',
    #     'port': 5432,
    #     'client_id': 2,
    #     'action': 'cancel_record_google_excel'
    # }
    data = request.get_json()
    action = data.get('action')
    client_id = data.get('client_id')
    paramsDb = {
        'database': data.get('database'),
        'user': data.get('user'),
        'password': data.get('password'),
        'host': data.get('host'),
        'port': data.get('port')
    }

    connection = psycopg2.connect(**paramsDb)

    cursor = connection.cursor()
    cursor.execute('select t.type, a.key, a.type_row,'
                   '             dp.database, dp.login, dp.password, dp.host, dp.port, dp.schema, dp.table,'
                   '             ep.sheet_id, ep.range, ep.value_input_option,'
                   '             rp.url, rp.type,'
                   '             (select json_agg(af) as item from n8n_schema.actions_fields af where af.action_id = a.id) as fields'
                   '  from n8n_schema.clients_integrations s '
                   '  join n8n_schema.integration_types t on s.integration_type_id = t.id'
                   '  join n8n_schema.actions a on a.id = s.action_id'
                   '  left join n8n_schema.integration_db_params dp on dp.client_integration_id = s.id'
                   '  left join n8n_schema.integration_excel_params ep on ep.client_integration_id = s.id'
                   '  left join n8n_schema.integration_rest_params rp on rp.client_integration_id = s.id'
                   ' where s.client_id = %s and a.key = %s', (client_id, action))

    if cursor.rowcount == 0:
        return {'status': SUCCESS, 'message': 'integrations not found'}

    rows = cursor.fetchall()
    connection.close()
    result = []
    for param in rows:
        if param[0] == API_EXCEL:
            result.append(processGoogleSheet(param, data))

        if param[0] == API_PG:
            result.append(processPg(param, data))

        if param[0] == API_REST:
            result.append(processRest(param))

    if len(result) == 0:
        return {'status': SUCCESS, 'message': 'integrations not found'}

    resultStatus = SUCCESS
    resultMessage = ''
    elem = Stream.of(result).filter(lambda f: f.get('status') == FAIL or f.get('status') == NOT_FOUND).find_first()
    if elem.is_present():
        resultStatus = FAIL
        resultMessage = elem.get().get('message')

    return {'status': resultStatus, 'message': resultMessage}


# Process google sheet
def processGoogleSheet(params, data):
    creds = getExcelCreds()

    if creds is None:
        return {'status': 'fail', 'message': 'excel credentials not found'}

    spreadsheet_id = params[10]
    value_input_option = params[12]
    range_name = params[11]

    service = build("sheets", "v4", credentials=creds)

    result = None
    if params[2] == INSERT:
        result = addRecordExcel(service, spreadsheet_id, value_input_option, range_name, data)

    if params[2] == DELETE:
        result = cancelRecordExcel(service, spreadsheet_id, data)

    return result


# Process postgres
def processPg(params, data):
    paramsDb = {
        'database': params[3],
        'user': params[4],
        'password': params[5],
        'host': params[6],
        'port': params[7]
    }

    connection = psycopg2.connect(**paramsDb)
    cursor = connection.cursor()

    result = None
    if params[2] == INSERT:
        result = addRecordPg(cursor, data, params)

    if params[2] == DELETE:
        result = cancelRecordPg(cursor, data, params)

    connection.commit()
    return result


# Process rest
def processRest(params):
    return {'status': SUCCESS, 'type': API_REST}


# @app.get('/api/creds')
# def creds():
#     getExcelCreds()
#     return {'status': SUCCESS}


if __name__ == '__main__':
    app.run()
