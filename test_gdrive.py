from google.oauth2 import service_account
from googleapiclient.discovery import build

try:
    credentials = service_account.Credentials.from_service_account_file(
        'config/credentials/googledrive.json',
        scopes=['https://www.googleapis.com/auth/drive.file']
    )
    
    service = build('drive', 'v3', credentials=credentials)
    about = service.about().get(fields='user').execute()
    print(f"✅ Google Drive authentication SUCCESS: {about['user']['emailAddress']}")
    
except Exception as e:
    print(f"❌ Google Drive test FAILED: {e}")
