#!/usr/bin/env python3

import pickle
import os.path
import pandas as pd

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
GOOGLE_API_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

#create here > https://developers.google.com/sheets/api/quickstart/go
GOOGLE_API_CREDENTIAL = 'google.api.credential.json'

def download_spreadsheet(spreadsheetID, rangeName):

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
      flow = InstalledAppFlow.from_client_secrets_file(GOOGLE_API_CREDENTIAL, GOOGLE_API_SCOPES)
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('token.pickle', 'wb') as token:
      pickle.dump(creds, token)

  service = build('sheets', 'v4', credentials=creds)

  sheet = service.spreadsheets()
  result = sheet.values().get(spreadsheetId=spreadsheetID, range=rangeName).execute()
  values = result.get('values', [])

  if not values:
    return None

  header = values.pop(0)
  return pd.DataFrame(values, columns=header)

