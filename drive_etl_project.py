from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine 
from sqlalchemy.engine import URL
from dotenv import load_dotenv
from datetime import datetime
import re 
import pandas as pd 
import io
import os
import os.path



def create_google_service(client_secret_file, api_name, api_version, scopes):
  """Create Connection to Google Drive API
  """
  CLIENT_SECRET_FILE = client_secret_file
  API_SERVICE_NAME = api_name
  API_VERSION = api_version
  SCOPES = scopes

  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          CLIENT_SECRET_FILE, SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())

  try:
    service = build(API_SERVICE_NAME, API_VERSION, credentials=creds)
    print("google service created successfully")
    return service
  except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    print(f"An error occurred: {error}")
    return None


def check_file(folder_ID, service):

    #send request to the api to list all the files in this folder_id
    query  = f"parents = '{folder_ID}'"
    response = service.files().list(q=query, fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)").execute()

    #using dictionary method get to returns the value of the item with the specified key. In this case value of files key
    files = response.get('files', [])

    data = []
    for row in files:
        data.append(row)


    df = pd.DataFrame(data)
    print(df)

    if df.empty:
        print("There is no file in this Google Drive")


def download_file(real_folder_id,service): #real_folder_id, real_file_id, service
    folder_id = real_folder_id

    query  = f"parents = '{folder_id}'"
    response = service.files().list(q=query, fields="nextPageToken, files(id, name)").execute()
    files = response.get('files', [])

    today_date = datetime.now().strftime("%Y-%m-%d")+".csv"
    for files in files:
        if today_date == files['name']:
            request = service.files().get_media(fileId=files['id'])
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False

            while not done:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}.")
            
            file.seek(0)

            with open(os.path.join('/root/drive_project/download_folder', files['name']), 'wb') as f:
                f.write(file.read())
                f.close()
            print('Download Today''s File Success')

def transformUserAgent(data):
    user_agent = data['UserAgent']
    browser_regex = r"(\w+)/\d+\.\d+"
    os_regex = r"\(([^;]+)"

    match_browser = re.search(browser_regex, user_agent)
    match_os = re.search(os_regex, user_agent)

    browser = match_browser.group(1) if match_browser else 'N/A'
    os = match_os.group(1) if match_os else 'N/A'

    new_columns = pd.Series({
        'browser': browser,
        'os': os
    })

    return new_columns

def etl(username, password):

    #Extract
    file_path = '/root/drive_project/download_folder/'
    file_name = datetime.now().strftime("%Y-%m-%d") + ".csv"
    file_path_name = "".join([file_path, file_name])
    raw_data = pd.read_csv(file_path_name)

    #Transform
    new_columns = raw_data.apply(transformUserAgent, axis = 1)
    transformed_data = pd.concat([raw_data, new_columns], axis=1)
    transformed_data = transformed_data.drop(['UserAgent'],axis = 1)

    #Load
    url_object = URL.create(
        drivername="postgresql",
        username="airflow",
        password="airflow",
        host="localhost",
        port="5434",
        database="postgres"
    )
     
    engine = create_engine(url_object)
    schema_name = 'public'
    table_name = 'users_visitors'
    transformed_data.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
    engine.dispose()



load_dotenv()
CLIENT_SECRET_FILE = os.getenv("CLIENT_SECRET_FILE")
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
API_NAME = "drive"
API_VERSION = "v3"
folder_id = os.getenv("FOLDER_ID")
file_id = os.getenv("FILE_ID")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


#Connect to Google Drive API
service = create_google_service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

#Check if file exist
# check_file(folder_id, service)

# #Download file
# download_file(folder_id,service)

# #ETL
# etl(POSTGRES_USERNAME, POSTGRES_PASSWORD)


with DAG(
    dag_id = 'drive_etl_project',
    start_date = datetime(2022,5,28),
    schedule = '00 21 * * *',
    catchup = True,
    max_active_runs=1
) as dag:

    operator_start_task = EmptyOperator(
        task_id = 'start'
    )

    operator_check_file = PythonOperator(
    task_id='check_if_file_exist',
    python_callable=check_file,
    op_args=[folder_id, service],
    )

    operator_download_file = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
        op_args=[folder_id, service],
    )

    operator_etl = PythonOperator(
        task_id='etl',
        python_callable=etl,
        op_args=[POSTGRES_USERNAME, POSTGRES_PASSWORD],
    )

    operator_end = EmptyOperator(
       task_id = 'end'
    )

operator_start_task >> operator_check_file >> operator_download_file >> operator_etl >> operator_end
