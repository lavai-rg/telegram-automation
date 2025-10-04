#!/usr/bin/env python3
"""
Telegram Music Channel Scraper
Scrapes music posts from Telegram channels and organizes them into cloud storage
"""

import asyncio
import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import aiohttp
import aiofiles
from pathlib import Path

# Required libraries
from telethon import TelegramClient
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import pandas as pd
from airtable import Airtable
import onedrivesdk
from onedrivesdk.helpers import GetAuthCodeServer

# Configuration
class Config:
    # Telegram API credentials
    API_ID = ""  # Get from my.telegram.org
    API_HASH = ""  # Get from my.telegram.org
    PHONE_NUMBER = ""
    
    # Google Drive API
    GOOGLE_DRIVE_CREDENTIALS_FILE = "credentials.json"
    GOOGLE_DRIVE_FOLDER_ID = ""
    
    # OneDrive API
    ONEDRIVE_CLIENT_ID = ""
    ONEDRIVE_CLIENT_SECRET = ""
    
    # Airtable
    AIRTABLE_API_KEY = ""
    AIRTABLE_BASE_ID = ""
    AIRTABLE_TABLE_NAME = "Music_Albums"
    
    # Google Sheets
    GOOGLE_SHEETS_ID = ""
    
    # Azure Storage (optional)
    AZURE_STORAGE_CONNECTION_STRING = ""

class TelegramMusicScraper:
    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('telegram_scraper.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    async def initialize_telegram_client(self):
        """Initialize Telegram client"""
        self.client = TelegramClient(
            'session', 
            self.config.API_ID, 
            self.config.API_HASH
        )
        await self.client.start(phone=self.config.PHONE_NUMBER)
        self.logger.info("Telegram client initialized successfully")
    
    async def scrape_channel_posts(self, channel_username: str, limit: int = 100) -> List[Dict]:
        """Scrape posts from Telegram channel"""
        posts_data = []
        
        try:
            entity = await self.client.get_entity(channel_username)
            
            async for message in self.client.iter_messages(entity, limit=limit):
                if message.media and hasattr(message.media, 'document'):
                    # Check if it's an audio file
                    if message.media.document.mime_type and 'audio' in message.media.document.mime_type:
                        post_data = await self._extract_post_data(message)
                        if post_data:
                            posts_data.append(post_data)
                            
        except Exception as e:
            self.logger.error(f"Error scraping channel {channel_username}: {e}")
            
        return posts_data
    
    async def _extract_post_data(self, message) -> Optional[Dict]:
        """Extract relevant data from message"""
        try:
            # Extract album info from message text
            text = message.text or ""
            
            # Parse album information
            album_info = self._parse_album_info(text)
            
            # Get file information
            file_info = {
                'file_id': message.media.document.id,
                'file_name': getattr(message.media.document, 'file_name', f"audio_{message.media.document.id}"),
                'file_size': message.media.document.size,
                'mime_type': message.media.document.mime_type,
                'date': message.date.isoformat()
            }
            
            # Combine information
            post_data = {
                **album_info,
                **file_info,
                'message_id': message.id,
                'raw_text': text
            }
            
            return post_data
            
        except Exception as e:
            self.logger.error(f"Error extracting post data: {e}")
            return None
    
    def _parse_album_info(self, text: str) -> Dict:
        """Parse album information from message text"""
        # Basic parsing logic - customize based on your channel's format
        lines = text.split('\n')
        
        album_info = {
            'artist': '',
            'album_name': '',
            'year': '',
            'genre': '',
            'description': text
        }
        
        # Simple parsing logic - adjust based on actual format
        for line in lines:
            line = line.strip()
            if ' - ' in line and not line.startswith('http'):
                parts = line.split(' - ', 1)
                if len(parts) == 2:
                    album_info['artist'] = parts[0].strip()
                    album_info['album_name'] = parts[1].strip()
                    break
        
        return album_info
    
    async def download_file(self, message, download_path: str) -> str:
        """Download file from Telegram"""
        try:
            file_path = await self.client.download_media(message, download_path)
            self.logger.info(f"Downloaded file: {file_path}")
            return file_path
        except Exception as e:
            self.logger.error(f"Error downloading file: {e}")
            return None
    
    def create_folder_structure(self, album_info: Dict, base_path: str) -> str:
        """Create folder structure for album"""
        artist = album_info.get('artist', 'Unknown Artist')
        album = album_info.get('album_name', 'Unknown Album')
        
        # Clean folder names
        artist = self._clean_filename(artist)
        album = self._clean_filename(album)
        
        folder_path = os.path.join(base_path, f"{artist} - {album}")
        os.makedirs(folder_path, exist_ok=True)
        
        return folder_path
    
    def _clean_filename(self, filename: str) -> str:
        """Clean filename from invalid characters"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '')
        return filename.strip()

class CloudStorageManager:
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    async def upload_to_google_drive(self, file_path: str, folder_name: str) -> Dict:
        """Upload file to Google Drive"""
        try:
            # Initialize Google Drive service
            service = self._get_google_drive_service()
            
            # Create folder if not exists
            folder_id = self._create_google_drive_folder(service, folder_name)
            
            # Upload file
            file_metadata = {
                'name': os.path.basename(file_path),
                'parents': [folder_id]
            }
            
            media = MediaFileUpload(file_path)
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,webViewLink'
            ).execute()
            
            return {
                'id': file['id'],
                'name': file['name'],
                'link': file['webViewLink']
            }
            
        except Exception as e:
            self.logger.error(f"Error uploading to Google Drive: {e}")
            return None
    
    def _get_google_drive_service(self):
        """Get Google Drive service"""
        # Implementation for Google Drive authentication
        pass
    
    def _create_google_drive_folder(self, service, folder_name: str) -> str:
        """Create folder in Google Drive"""
        # Implementation for folder creation
        pass
    
    async def upload_to_onedrive(self, file_path: str, folder_name: str) -> Dict:
        """Upload file to OneDrive"""
        # Implementation for OneDrive upload
        pass
    
    async def save_to_airtable(self, album_data: Dict) -> bool:
        """Save album data to Airtable"""
        try:
            airtable = Airtable(
                self.config.AIRTABLE_BASE_ID,
                self.config.AIRTABLE_TABLE_NAME,
                api_key=self.config.AIRTABLE_API_KEY
            )
            
            record = airtable.insert(album_data)
            self.logger.info(f"Saved to Airtable: {record['id']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving to Airtable: {e}")
            return False
    
    async def save_to_google_sheets(self, album_data: List[Dict]) -> bool:
        """Save album data to Google Sheets"""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(album_data)
            
            # Save to Google Sheets (implementation needed)
            # Use gspread library for easier Google Sheets integration
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving to Google Sheets: {e}")
            return False

class AzureVMManager:
    """Azure VM management for scalable processing"""
    
    def __init__(self, subscription_id: str, resource_group: str):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
    
    async def create_vm_for_scraping(self) -> Dict:
        """Create Azure VM optimized for scraping"""
        vm_config = {
            'size': 'Standard_D2s_v3',  # 2 vCPUs, 8GB RAM
            'os': 'Ubuntu 20.04 LTS',
            'storage': 'Premium SSD 128GB',
            'network': 'Standard public IP'
        }
        
        # Implementation using Azure SDK
        return vm_config
    
    async def setup_scraping_environment(self, vm_name: str):
        """Setup scraping environment on Azure VM"""
        setup_script = """
        #!/bin/bash
        # Update system
        sudo apt update && sudo apt upgrade -y
        
        # Install Python and pip
        sudo apt install python3 python3-pip -y
        
        # Install required packages
        pip3 install telethon aiohttp aiofiles pandas google-api-python-client airtable-python-wrapper
        
        # Install Azure CLI
        curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
        
        # Setup firewall
        sudo ufw enable
        sudo ufw allow ssh
        
        # Download and setup scraper
        git clone <your-repo-url>
        cd telegram-music-scraper
        pip3 install -r requirements.txt
        """
        
        return setup_script

async def main():
    """Main execution function"""
    config = Config()
    
    # Initialize scraper
    scraper = TelegramMusicScraper(config)
    await scraper.initialize_telegram_client()
    
    # Initialize cloud storage manager
    storage_manager = CloudStorageManager(config)
    
    # Target channel
    channel_username = "pustaka_musik_dan_lagu"  # From your screenshot
    
    try:
        # Scrape posts
        posts = await scraper.scrape_channel_posts(channel_username, limit=50)
        
        for post in posts:
            # Create local folder structure
            album_folder = scraper.create_folder_structure(post, "./downloads")
            
            # Download file (if needed)
            # file_path = await scraper.download_file(message, album_folder)
            
            # Upload to cloud storages
            # gdrive_result = await storage_manager.upload_to_google_drive(file_path, album_folder)
            # onedrive_result = await storage_manager.upload_to_onedrive(file_path, album_folder)
            
            # Save metadata to databases
            await storage_manager.save_to_airtable(post)
        
        # Save all data to Google Sheets
        await storage_manager.save_to_google_sheets(posts)
        
        print(f"Successfully processed {len(posts)} posts")
        
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
    
    finally:
        if scraper.client:
            await scraper.client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())