from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import base64

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

"""Shows basic usage of the Gmail API.
Lists the user's Gmail labels.
"""

def matching():
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

    search_query = "Project Loan Daily Reporting"
    result = service.users().messages().list(userId='me', q=search_query).execute()
    msgs = result['messages']
    msg_ids = [msg['id'] for msg in msgs]

    messageId = msg_ids[0]
    msg = service.users().messages().get(userId='me', id=messageId).execute()
    sg = service.users().messages().get(userId='me', id=messageId).execute()
    parts = msg.get('payload').get('parts')
    all_parts = []
    for p in parts:
        if p.get('parts'):
            all_parts.extend(p.get('parts'))
        else:
            all_parts.append(p)


    messageId = msg_ids[0]
    data = p['body'].get('data')
    attachmentId = p['body'].get('attachmentId')
    if not data:
        att = service.users().messages().attachments().get(
                userId='me', id=attachmentId, messageId=messageId).execute()
        data = att['data']
    str_csv  = base64.urlsafe_b64decode(data.encode('UTF-8'))
    hd_file  = pd.read_excel(str_csv)

    # prepare data for merging
    hd_file.loc[hd_file['Store Name']=='Online','Store #']='E-Commerce'

    hd_file['Match_Code']=hd_file['Store #'].astype(str)+'_'+hd_file['CARDHLDR_ACCT_NBR'].astype(str)+'_'+abs(hd_file['PAYMT_AMT']).astype(str)

    print(hd_file.head(10))

if __name__=="__main__":
    matching()