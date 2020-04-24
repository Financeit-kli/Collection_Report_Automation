from __future__ import print_function
import pickle
import os.path
from email.mime.text import MIMEText

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import base64
from datetime import date, timedelta
from io import BytesIO

SCOPES = ['https://mail.google.com/']
GSCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1IStyQ5gwwnKJQ8Q3OpEkzX17Z_VoCKuI9FQEN8eOg_0'


def read_from_gmail():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)

    # Call the Gmail API


    queries = ["REP-1500-1", "REP-1500-2", "REP-1500-3", "REP-1500-4", "REP-1500-5", "REP-1500-6", "REP-1500-7", "REP-1500-8", "REP-1500-9"]
    wip = []
    updates = []
    for query in queries:

        result = service.users().messages().list(userId='me', q=query).execute()
        msgs = result['messages']
        msg_ids = [msg['id'] for msg in msgs]

        messageId = msg_ids[0]
        msg = service.users().messages().get(userId='me', id=messageId).execute()
        parts = msg.get('payload').get('parts')
        all_parts = []
        for p in parts:
            if p.get('parts'):
                all_parts.extend(p.get('parts'))
            else:
                all_parts.append(p)

        data = p['body'].get('data')
        attachmentId = p['body'].get('attachmentId')

        if not data:
            att = service.users().messages().attachments().get(
                    userId='me', id=attachmentId, messageId=messageId).execute()
            data = att['data']
        str_csv = base64.urlsafe_b64decode(data.encode('UTF-8'))
        update = pd.read_csv(BytesIO(str_csv))
        wip.append(update)
    # Part 1 Arrangement by Status
    updates.append(update.iloc[-1].dropna().values.tolist())

    # Part 2 Outreach data. Merged results cannot be sent through email so I have to read separately and merge them

    media.extend(update.iloc[-1].dropna().values.tolist())
    media = [float(media[0]),float(media[1]),float(media[2]),media[1]/media[0],(media[2]/media[1])-1,float(media[3])+float(media[4])]
    updates.append(media)

    # Part 3 Active Collection Files. Needs to reformat to fits in the report and avoid null value

    updates.append(update.drop([0]).drop(['Days in Arrears Buckets'], axis=1).fillna(0).values.flatten().tolist())

    # notice = MIMEText('Report has been updated')
    # notice['to'] = 'kli@financeit.io;gracine@financeit.io'
    # notice['from'] = 'kli@financeit.io'
    # notice['subject'] = 'Report has been updated'
    # raw_message = base64.urlsafe_b64encode(notice.as_string().encode("utf-8"))
    # message = {'raw': raw_message.decode("utf-8")}
    send_message = (service.users().messages().send(userId='me', body=message)
               .execute())
    # write_to_sheets(updates)



def write_to_sheets(updates):
    creds = None

    if os.path.exists('gtoken.pickle'):
        with open('gtoken.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'gcredentials.json', GSCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('gtoken.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # get first empty line
    line = int(service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range='Outreach Data!A3'
            ).execute().get('values')[0][0])+28

    arrangementone = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            valueInputOption='RAW',
            range='Arrangement Data!B'+str(line),
            body=dict(
                values=[updates[0]]
        )).execute()

    arrangementtwo = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        valueInputOption='RAW',
        range='Arrangement Data!K'+str(line),
        body=dict(
            values=[updates[1]]
        )).execute()

    outreachone = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        valueInputOption='RAW',
        range='Outreach Data!I'+str(line),
        body=dict(
            values=[updates[3]]
        )).execute()

    outreachtwo = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        valueInputOption='RAW',
        range='Outreach Data!AG'+str(line),
        body=dict(
            values=[updates[4]]
        )).execute()

    outreachthree = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        valueInputOption='RAW',
        range='Outreach Data!B'+str(line),
        body=dict(
            values=[updates[2]]
        )).execute()

    yesterday = date.today() - timedelta(days=1)
    adddate = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        valueInputOption='RAW',
        range='Outreach Data!A'+str(line),
        body=dict(
            values=[[yesterday.strftime("%b %d %Y")]]
        )).execute()


read_from_gmail()
