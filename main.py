from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

import json
import os

DRIVE_FILE = open(os.path.join('config', 'drive.json'))
SERVICE_ACCOUNT_FILE_PATH = os.path.join('config', 'service_account.json')
DRIVE_CONFIG = json.load(DRIVE_FILE)
OAUTH_FILE_PATH = os.path.join('config', 'credentials.json')
TOKEN_FILE_PATH = os.path.join('config', 'token.json')

SOURCE_FOLDER_ID = str(DRIVE_CONFIG['source_folder_id'])
DESTINATION_FOLDER_ID = str(DRIVE_CONFIG['destination_folder_id'])

SCOPES=['https://www.googleapis.com/auth/drive']
PORT=8000


def initialize_service_account(cred_file, service):

    match service:
        case "drive":
            scope_list = ['https://www.googleapis.com/auth/drive']
            version = 'v3'
            service_name = 'drive'
        case "sheets":
            scope_list = ['https://www.googleapis.com/auth/spreadsheets']
            version = 'v3'
            service_name = 'spreadsheets'
        case _:
            scope_list = ['https://www.googleapis.com/auth/drive']
            version = 'v3'
            service_name = 'drive'

    try:
        cred = service_account.Credentials.from_service_account_file(
            cred_file, scopes=scope_list
        )
        svc = build(service_name, version, credentials=cred)
        return svc
    except HttpError as error:
        return error


def initialize_oauth_flow():
    try:
        oauth_creds = None
        if os.path.exists(TOKEN_FILE_PATH):
            oauth_creds = Credentials.from_authorized_user_file(TOKEN_FILE_PATH, SCOPES)
        if not oauth_creds or not oauth_creds.valid:
            if oauth_creds and oauth_creds.expired and oauth_creds.refresh_token:
                oauth_creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    OAUTH_FILE_PATH, SCOPES
                )
                oauth_creds=flow.run_local_server(port=PORT)
                with open(TOKEN_FILE_PATH, 'w') as token:
                    token.write(oauth_creds.to_json())

        return oauth_creds
    except HttpError as error:
        return error


def construct_drive_service(creds):
    try:
        svc = build('drive', 'v3', credentials=creds)
        return svc
    except HttpError as error:
        return error


def get_drive_file_by_id(service, file_id):
    try:
        file_info = service.files().get(fileId=file_id, fields='id, name, mimeType').execute()
        return file_info
    except HttpError as error:
        return error


def get_drive_child_item(service, parent_id, query=None):
    if not query:
        query = f"'{parent_id}' in parents"
    try:
        response = service.files().list(q=query, fields='files(id, name, mimeType)').execute()
        files = response.get('files', [])
        return files
    except HttpError as error:
        return error


def get_child_item_recursive(service, folder_id):
    try:
        query = f"'{folder_id}' in parents"
        response = service.files().list(q=query, fields='files(id, name, mimeType, parents)').execute()

        items_in_folder = response.get('files', [])
        final_list = []

        for item in items_in_folder:
            final_list.append({'id': item['id'], 'name': item['name'], 'mimeType': item['mimeType'], 'parentID': item['parents']})
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                final_list.extend(get_child_item_recursive(service=service, folder_id=item['id']))

        return final_list

    except Exception as e:
        print(f"ERROR!  {e}")
        return []


def get_drive_child_item_count(service, parent_id):
    parent_info = get_drive_file_by_id(service, parent_id)
    child_items = get_drive_child_item(service, parent_id)

    data = {'parentID': parent_id, 'parentName': parent_info['name'], 'childItemCount': len(child_items)}

    return json.dumps(data)


def get_drive_recursive_child_item_report(service, folder_id):
    report = []
    folder_query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
    top_level_folders = get_drive_child_item(
        service=service, parent_id=folder_id, query=folder_query
    )

    for folder in top_level_folders:
        folder_item_count = len(get_child_item_recursive(folder_id=folder['id'], service=service))
        report.append({'id': folder['id'], 'name': folder['name'], 'item_count': folder_item_count})

    return json.dumps(report)


def copy_drive_content(service, source, destination):

    # Get items from source folder
    source_items = get_drive_child_item(service=service, parent_id=source)

    for item in source_items:
        # If an item is a folder, create a folder that matches the name and folder structure of the source
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            new_folder_data = {
                'name': item['name'],
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [destination]
            }
            new_folder = service.files().create(body=new_folder_data).execute()
            # Recursively copy contents of folder
            copy_drive_content(service=service, source=item['id'], destination=new_folder['id'])
        # If an item is anything other than a folder, copy it
        else:
            copy_file_data = {
                'name': item['name'],
                'parents': [destination]
            }
            service.files().copy(fileId=item['id'], body=copy_file_data).execute()


if __name__ == '__main__':
    oauth_credentials = initialize_oauth_flow()
    drive_service = construct_drive_service(oauth_credentials)

    # Assessment 1
    # Write a script to generate a report that shows the number of
    # files and folders in total at the root of the source folder.
    print(get_drive_child_item_count(service=drive_service, parent_id=SOURCE_FOLDER_ID))

    # Assessment 2
    # Write a script to generate a report that shows the number of child objects (recursively)
    # for each top-level folder under the source folder id and a total of nested folders for the source folder.
    print(get_drive_recursive_child_item_report(service=drive_service, folder_id=SOURCE_FOLDER_ID))

    # Assessment 3
    # Write a script to copy the content (nested files/folders) of the source folder to the destination folder.
    # copy_drive_content(service=drive_service, source=SOURCE_FOLDER_ID, destination=DESTINATION_FOLDER_ID)
