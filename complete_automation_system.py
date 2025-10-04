#!/usr/bin/env python3

"""
Complete Telegram Music Automation System
Handles: Scraping, Forwarding, Cloud Upload, Database Sync
Target: @IndoGlobalMusikAmbulu complete history processing
"""

import asyncio
import os
import sys
import logging
import json
import time
import shutil
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import sqlite3
from dataclasses import dataclass, asdict

# Core imports
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaDocument, DocumentAttributeAudio
from telethon.errors.rpcerrorlist import FloodWaitError
import asyncio
import requests
import schedule
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Cloud storage imports
try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    import gspread
except ImportError:
    print("Google APIs not installed. Install with: pip install google-api-python-client google-auth")

try:
    from airtable import Airtable
except ImportError:
    print("Airtable not installed. Install with: pip install airtable-python-wrapper")

# Audio processing
try:
    from mutagen import File as MutagenFile
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC
except ImportError:
    print("Mutagen not installed. Install with: pip install mutagen")

# Load environment variables
from dotenv import load_dotenv
load_dotenv('config/config.env')

@dataclass
class MusicTrack:
    """Data structure for music track information"""
    file_id: str
    title: str
    artist: str
    album: str
    year: str
    side: str  # "Side A" or "Side B"
    duration: int
    file_size: int
    file_path: str
    download_url: str
    message_id: int
    channel_id: str
    upload_date: datetime
    forwarded_date: Optional[datetime] = None
    gdrive_url: Optional[str] = None
    onedrive_url: Optional[str] = None
    airtable_id: Optional[str] = None
    sheets_row: Optional[int] = None
    processing_status: str = "pending"  # pending, downloaded, organized, uploaded, completed, failed

class TelegramMusicArchiver:
    """Complete automation system for Telegram music channel archiving"""
    
    def __init__(self):
        self.setup_logging()
        self.load_config()
        self.setup_database()
        self.setup_directories()
        
        # Initialize clients
        self.telegram_client = None
        self.gdrive_service = None
        self.gsheets_client = None
        self.airtable_client = None
        
        # State tracking
        self.stats = {
            'total_processed': 0,
            'successful_downloads': 0,
            'successful_uploads': 0,
            'failed_operations': 0,
            'start_time': datetime.now()
        }
        
        self.executor = ThreadPoolExecutor(max_workers=int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '3')))
        
    def setup_logging(self):
        """Setup comprehensive logging system"""
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        # Create log subdirectories
        for subdir in ['scraping', 'forwarding', 'upload', 'database', 'system']:
            (log_dir / subdir).mkdir(exist_ok=True)
        
        # Main logger
        self.logger = logging.getLogger('TelegramMusicArchiver')
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler('logs/system/main.log')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
    def load_config(self):
        """Load configuration from environment variables"""
        self.config = {
            # Telegram
            'api_id': os.getenv('TELEGRAM_API_ID'),
            'api_hash': os.getenv('TELEGRAM_API_HASH'),
            'bot_token': os.getenv('TELEGRAM_BOT_TOKEN'),
            'phone_number': os.getenv('TELEGRAM_PHONE_NUMBER'),
            'source_channel': os.getenv('SOURCE_CHANNEL', 'IndoGlobalMusikAmbulu'),
            'private_channel_id': os.getenv('PRIVATE_CHANNEL_ID'),
            
            # Cloud storage
            'gdrive_credentials': os.getenv('GOOGLE_DRIVE_CREDENTIALS_PATH'),
            'gdrive_folder_id': os.getenv('GOOGLE_DRIVE_FOLDER_ID'),
            'gsheets_credentials': os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH'),
            'gsheets_id': os.getenv('GOOGLE_SHEETS_ID'),
            'airtable_api_key': os.getenv('AIRTABLE_API_KEY'),
            'airtable_base_id': os.getenv('AIRTABLE_BASE_ID'),
            'airtable_table': os.getenv('AIRTABLE_TABLE_NAME', 'MusicTracks'),
            
            # Processing
            'download_path': Path(os.getenv('DOWNLOAD_PATH', 'downloads')),
            'max_concurrent': int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '3')),
            'download_timeout': int(os.getenv('DOWNLOAD_TIMEOUT', '300')),
            'retry_attempts': int(os.getenv('RETRY_ATTEMPTS', '3')),
            'batch_size': int(os.getenv('BATCH_SIZE', '50')),
            
            # Organization
            'organize_by_album': os.getenv('ORGANIZE_BY_ALBUM', 'true').lower() == 'true',
            'create_side_folders': os.getenv('CREATE_SIDE_AB_FOLDERS', 'true').lower() == 'true'
        }
        
        # Validate required config
        required_fields = ['api_id', 'api_hash', 'source_channel']
        for field in required_fields:
            if not self.config.get(field):
                raise ValueError(f"Missing required configuration: {field}")
                
        self.logger.info("Configuration loaded successfully")
        
    def setup_database(self):
        """Setup SQLite database for tracking"""
        db_path = Path('data/music_tracks.db')
        db_path.parent.mkdir(exist_ok=True)
        
        self.db_connection = sqlite3.connect(db_path, check_same_thread=False)
        self.db_connection.execute('''
            CREATE TABLE IF NOT EXISTS music_tracks (
                file_id TEXT PRIMARY KEY,
                title TEXT,
                artist TEXT,
                album TEXT,
                year TEXT,
                side TEXT,
                duration INTEGER,
                file_size INTEGER,
                file_path TEXT,
                download_url TEXT,
                message_id INTEGER,
                channel_id TEXT,
                upload_date TEXT,
                forwarded_date TEXT,
                gdrive_url TEXT,
                onedrive_url TEXT,
                airtable_id TEXT,
                sheets_row INTEGER,
                processing_status TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.db_connection.execute('''
            CREATE TABLE IF NOT EXISTS processing_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT,
                operation TEXT,
                status TEXT,
                message TEXT,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.db_connection.commit()
        self.logger.info("Database initialized successfully")
        
    def setup_directories(self):
        """Create necessary directory structure"""
        base_path = self.config['download_path']
        
        directories = [
            base_path / 'raw',
            base_path / 'organized',
            base_path / 'processed',
            base_path / 'failed',
            Path('temp'),
            Path('data'),
            Path('backups')
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            
        self.logger.info("Directory structure created")
        
    async def initialize_clients(self):
        """Initialize all API clients"""
        try:
            # Initialize Telegram client
            self.telegram_client = TelegramClient(
                'config/credentials/telegram_session',
                int(self.config['api_id']),
                self.config['api_hash']
            )
            
            await self.telegram_client.start(phone=self.config['phone_number'])
            self.logger.info("Telegram client initialized")
            
            # Initialize Google Drive
            if self.config.get('gdrive_credentials'):
                self.setup_google_drive()
                
            # Initialize Google Sheets
            if self.config.get('gsheets_credentials'):
                self.setup_google_sheets()
                
            # Initialize Airtable
            if self.config.get('airtable_api_key'):
                self.setup_airtable()
                
        except Exception as e:
            self.logger.error(f"Failed to initialize clients: {e}")
            raise
            
    def setup_google_drive(self):
        """Setup Google Drive client"""
        try:
            credentials = Credentials.from_service_account_file(
                self.config['gdrive_credentials'],
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.gdrive_service = build('drive', 'v3', credentials=credentials)
            self.logger.info("Google Drive client initialized")
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            
    def setup_google_sheets(self):
        """Setup Google Sheets client"""
        try:
            self.gsheets_client = gspread.service_account(
                filename=self.config['gsheets_credentials']
            )
            self.logger.info("Google Sheets client initialized")
        except Exception as e:
            self.logger.error(f"Failed to setup Google Sheets: {e}")
            
    def setup_airtable(self):
        """Setup Airtable client"""
        try:
            self.airtable_client = Airtable(
                self.config['airtable_base_id'],
                self.config['airtable_table'],
                api_key=self.config['airtable_api_key']
            )
            self.logger.info("Airtable client initialized")
        except Exception as e:
            self.logger.error(f"Failed to setup Airtable: {e}")
            
    def parse_audio_metadata(self, file_path: Path, message_text: str = "") -> Dict:
        """Extract and parse audio metadata"""
        try:
            # Use mutagen to read audio metadata
            audio_file = MutagenFile(file_path)
            
            metadata = {
                'title': '',
                'artist': '',
                'album': '',
                'year': '',
                'duration': 0
            }
            
            if audio_file:
                # Extract from ID3 tags
                if hasattr(audio_file, 'tags') and audio_file.tags:
                    metadata['title'] = str(audio_file.tags.get('TIT2', [''])[0]) if 'TIT2' in audio_file.tags else ''
                    metadata['artist'] = str(audio_file.tags.get('TPE1', [''])[0]) if 'TPE1' in audio_file.tags else ''
                    metadata['album'] = str(audio_file.tags.get('TALB', [''])[0]) if 'TALB' in audio_file.tags else ''
                    metadata['year'] = str(audio_file.tags.get('TDRC', [''])[0]) if 'TDRC' in audio_file.tags else ''
                
                # Get duration
                if hasattr(audio_file, 'info') and audio_file.info:
                    metadata['duration'] = int(getattr(audio_file.info, 'length', 0))
            
            # Parse from message text if metadata is missing
            if not metadata['title'] and message_text:
                metadata.update(self.parse_message_text(message_text))
                
            return metadata
            
        except Exception as e:
            self.logger.error(f"Failed to parse audio metadata for {file_path}: {e}")
            return {'title': '', 'artist': '', 'album': '', 'year': '', 'duration': 0}
            
    def parse_message_text(self, text: str) -> Dict:
        """Parse artist, album, title from message text"""
        metadata = {'title': '', 'artist': '', 'album': '', 'year': ''}
        
        try:
            # Common patterns for music posts
            patterns = [
                r'(?P<artist>.*?)\s*[-–—]\s*(?P<title>.*?)\s*\((?P<year>\d{4})\)',
                r'(?P<artist>.*?)\s*[-–—]\s*(?P<album>.*?)\s*[-–—]\s*(?P<title>.*)',
                r'(?P<artist>.*?)\s*:\s*(?P<title>.*)',
                r'(?P<title>.*?)\s*by\s*(?P<artist>.*)',
            ]
            
            import re
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    metadata.update({k: v.strip() for k, v in match.groupdict().items() if v})
                    break
                    
            # Extract year if not found
            if not metadata['year']:
                year_match = re.search(r'\b(19|20)\d{2}\b', text)
                if year_match:
                    metadata['year'] = year_match.group()
                    
        except Exception as e:
            self.logger.error(f"Failed to parse message text: {e}")
            
        return metadata
        
    def determine_side(self, title: str, album: str) -> str:
        """Determine if track belongs to Side A or Side B"""
        title_lower = title.lower()
        album_lower = album.lower()
        
        # Keywords that typically indicate Side B
        side_b_keywords = [
            'side b', 'b-side', 'bside', 'flip', 'instrumental',
            'remix', 'version', 'alternate', 'demo', 'unreleased'
        ]
        
        for keyword in side_b_keywords:
            if keyword in title_lower or keyword in album_lower:
                return "Side B"
                
        return "Side A"  # Default to Side A
        
    def organize_file_path(self, track: MusicTrack) -> Path:
        """Generate organized file path following Album/Side A/Side B structure"""
        base_path = self.config['download_path'] / 'organized'
        
        # Sanitize names for file system
        def sanitize_name(name: str) -> str:
            import re
            # Remove invalid characters
            name = re.sub(r'[<>:"/\\|?*]', '', name)
            name = re.sub(r'[\x00-\x1f]', '', name)  # Remove control characters
            return name.strip()
            
        artist = sanitize_name(track.artist) or "Unknown Artist"
        album = sanitize_name(track.album) or "Unknown Album"
        year = track.year if track.year else ""
        
        # Create album folder name
        if year:
            album_folder = f"{artist} - {album} ({year})"
        else:
            album_folder = f"{artist} - {album}"
            
        # Determine file extension
        original_path = Path(track.file_path)
        file_extension = original_path.suffix
        
        # Create organized path
        if self.config['create_side_folders']:
            organized_path = base_path / album_folder / track.side / f"{sanitize_name(track.title)}{file_extension}"
        else:
            organized_path = base_path / album_folder / f"{sanitize_name(track.title)}{file_extension}"
            
        return organized_path
        
    async def process_channel_history(self):
        """Process complete channel history with FloodWait handling"""
        self.logger.info(f"Starting complete history processing for @{self.config['source_channel']}")
        
        while True:
            try:
                entity = await self.telegram_client.get_entity(self.config['source_channel'])
                
                # Get total message count for progress tracking
                total_messages = 0
                async for message in self.telegram_client.iter_messages(entity, limit=None):
                    total_messages += 1
                    
                self.logger.info(f"Found {total_messages} total messages to process")
                
                processed_count = 0
                batch = []
                
                # Process messages in batches
                async for message in self.telegram_client.iter_messages(entity, limit=None):
                    try:
                        if self.is_audio_message(message):
                            track = await self.extract_track_info(message)
                            if track:
                                batch.append(track)
                                
                        # Process batch when full
                        if len(batch) >= self.config['batch_size']:
                            await self.process_batch(batch)
                            batch = []
                            
                        processed_count += 1
                        
                        # Progress logging
                        if processed_count % 100 == 0:
                            progress = (processed_count / total_messages) * 100
                            self.logger.info(f"Progress: {processed_count}/{total_messages} ({progress:.1f}%)")
                            
                    except FloodWaitError as e:
                        msg = f"[FLOODWAIT] Telegram requested wait for {e.seconds} seconds."
                        self.logger.warning(msg)
                        logging.info(msg)
                        await asyncio.sleep(e.seconds + 5)
                    except Exception as e:
                        self.logger.error(f"Error processing message {message.id}: {e}")
                        continue
                        
                # Process remaining batch
                if batch:
                    await self.process_batch(batch)
                    
                self.logger.info(f"Completed processing {processed_count} messages")
                break  # Exit loop if no FloodWaitError occurs
                
            except FloodWaitError as e:
                msg = f"[FLOODWAIT] Main loop paused for {e.seconds} seconds."
                self.logger.warning(msg)
                logging.info(msg)
                await asyncio.sleep(e.seconds + 5)
            except Exception as e:
                self.logger.error(f"Failed to process channel history: {e}")
                raise
            
    def is_audio_message(self, message) -> bool:
        """Check if message contains audio file"""
        if not message.media:
            return False
            
        if isinstance(message.media, MessageMediaDocument):
            document = message.media.document
            
            # Check MIME type
            if document.mime_type and document.mime_type.startswith('audio/'):
                return True
                
            # Check file attributes
            for attr in document.attributes:
                if isinstance(attr, DocumentAttributeAudio):
                    return True
                    
        return False
        
    async def extract_track_info(self, message) -> Optional[MusicTrack]:
        """Extract track information from Telegram message"""
        try:
            if not self.is_audio_message(message):
                return None
                
            document = message.media.document
            
            # Get basic file info
            file_id = str(document.id)
            file_size = document.size
            
            # Get audio attributes
            duration = 0
            title = ""
            artist = ""
            
            for attr in document.attributes:
                if isinstance(attr, DocumentAttributeAudio):
                    duration = attr.duration
                    if hasattr(attr, 'title') and attr.title:
                        title = attr.title
                    if hasattr(attr, 'performer') and attr.performer:
                        artist = attr.performer
                    break
                    
            # Parse metadata from message text
            message_text = message.message or ""
            parsed_metadata = self.parse_message_text(message_text)
            
            # Combine metadata (prioritize parsed over Telegram attributes)
            final_title = parsed_metadata.get('title') or title or f"Track_{file_id}"
            final_artist = parsed_metadata.get('artist') or artist or "Unknown Artist"
            final_album = parsed_metadata.get('album') or "Unknown Album"
            final_year = parsed_metadata.get('year') or ""
            
            # Determine side
            side = self.determine_side(final_title, final_album)
            
            track = MusicTrack(
                file_id=file_id,
                title=final_title,
                artist=final_artist,
                album=final_album,
                year=final_year,
                side=side,
                duration=duration,
                file_size=file_size,
                file_path="",  # Will be set after download
                download_url="",  # Will be generated during download
                message_id=message.id,
                channel_id=str(message.peer_id.channel_id),
                upload_date=message.date
            )
            
            return track
            
        except Exception as e:
            self.logger.error(f"Failed to extract track info from message {message.id}: {e}")
            return None
            
    async def process_batch(self, tracks: List[MusicTrack]):
        """Process a batch of tracks"""
        self.logger.info(f"Processing batch of {len(tracks)} tracks")
        
        # Save tracks to database
        for track in tracks:
            self.save_track_to_db(track)
            
        # Download tracks concurrently
        download_tasks = []
        for track in tracks:
            task = self.executor.submit(self.download_track, track)
            download_tasks.append(task)
            
        # Wait for downloads to complete
        for future in as_completed(download_tasks):
            try:
                track = future.result()
                if track:
                    # Process downloaded track
                    await self.process_downloaded_track(track)
            except Exception as e:
                self.logger.error(f"Download task failed: {e}")
                
    def download_track(self, track: MusicTrack) -> Optional[MusicTrack]:
        """Download individual track"""
        try:
            # Create download path
            raw_path = self.config['download_path'] / 'raw' / f"{track.file_id}.mp3"
            
            # Skip if already downloaded
            if raw_path.exists():
                track.file_path = str(raw_path)
                track.processing_status = "downloaded"
                self.update_track_in_db(track)
                return track
                
            # Download using Telegram client
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            async def download():
                await self.telegram_client.download_media(
                    message_id=track.message_id,
                    file=raw_path
                )
                
            loop.run_until_complete(download())
            loop.close()
            
            if raw_path.exists():
                track.file_path = str(raw_path)
                track.processing_status = "downloaded"
                self.update_track_in_db(track)
                self.stats['successful_downloads'] += 1
                self.logger.info(f"Downloaded: {track.title}")
                return track
            else:
                raise Exception("Download failed - file not found")
                
        except Exception as e:
            self.logger.error(f"Failed to download {track.title}: {e}")
            track.processing_status = "failed"
            self.update_track_in_db(track)
            self.stats['failed_operations'] += 1
            return None
            
    async def process_downloaded_track(self, track: MusicTrack):
        """Process downloaded track: organize, upload, sync"""
        try:
            # 1. Organize file
            organized_track = await self.organize_track(track)
            
            # 2. Upload to cloud storage
            if organized_track:
                await self.upload_to_cloud(organized_track)
                
            # 3. Sync to databases
            if organized_track:
                await self.sync_to_databases(organized_track)
                
            # 4. Forward to private channel
            if organized_track and self.config.get('private_channel_id'):
                await self.forward_to_private_channel(organized_track)
                
        except Exception as e:
            self.logger.error(f"Failed to process downloaded track {track.title}: {e}")
            
    async def organize_track(self, track: MusicTrack) -> Optional[MusicTrack]:
        """Organize track into proper folder structure"""
        try:
            if not track.file_path or not os.path.exists(track.file_path):
                return None
                
            # Parse audio metadata from file
            audio_metadata = self.parse_audio_metadata(Path(track.file_path))
            
            # Update track info with parsed metadata
            if audio_metadata.get('title'):
                track.title = audio_metadata['title']
            if audio_metadata.get('artist'):
                track.artist = audio_metadata['artist']
            if audio_metadata.get('album'):
                track.album = audio_metadata['album']
            if audio_metadata.get('year'):
                track.year = audio_metadata['year']
            if audio_metadata.get('duration'):
                track.duration = audio_metadata['duration']
                
            # Determine organized path
            organized_path = self.organize_file_path(track)
            
            # Create directory structure
            organized_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Move file to organized location
            shutil.move(track.file_path, organized_path)
            track.file_path = str(organized_path)
            track.processing_status = "organized"
            
            self.update_track_in_db(track)
            self.logger.info(f"Organized: {track.title} -> {organized_path}")
            
            return track
            
        except Exception as e:
            self.logger.error(f"Failed to organize track {track.title}: {e}")
            track.processing_status = "failed"
            self.update_track_in_db(track)
            return None
            
    async def upload_to_cloud(self, track: MusicTrack):
        """Upload track to Google Drive and OneDrive"""
        try:
            # Upload to Google Drive
            if self.gdrive_service and self.config.get('gdrive_folder_id'):
                gdrive_url = await self.upload_to_gdrive(track)
                if gdrive_url:
                    track.gdrive_url = gdrive_url
                    
            # Upload to OneDrive (placeholder - implement based on your OneDrive setup)
            # onedrive_url = await self.upload_to_onedrive(track)
            # if onedrive_url:
            #     track.onedrive_url = onedrive_url
            
            track.processing_status = "uploaded"
            self.update_track_in_db(track)
            self.stats['successful_uploads'] += 1
            
        except Exception as e:
            self.logger.error(f"Failed to upload {track.title}: {e}")
            
    async def upload_to_gdrive(self, track: MusicTrack) -> Optional[str]:
        """Upload file to Google Drive"""
        try:
            file_path = Path(track.file_path)
            if not file_path.exists():
                return None
                
            # Create folder structure in Google Drive
            folder_id = await self.create_gdrive_folder_structure(track)
            
            # Upload file
            media = MediaFileUpload(str(file_path), resumable=True)
            file_metadata = {
                'name': file_path.name,
                'parents': [folder_id]
            }
            
            result = self.gdrive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,webViewLink'
            ).execute()
            
            self.logger.info(f"Uploaded to Google Drive: {track.title}")
            return result.get('webViewLink')
            
        except Exception as e:
            self.logger.error(f"Failed to upload to Google Drive {track.title}: {e}")
            return None
            
    async def create_gdrive_folder_structure(self, track: MusicTrack) -> str:
        """Create folder structure in Google Drive matching local organization"""
        try:
            # Get or create album folder
            album_name = f"{track.artist} - {track.album}"
            if track.year:
                album_name += f" ({track.year})"
                
            album_folder_id = self.get_or_create_gdrive_folder(
                album_name, self.config['gdrive_folder_id']
            )
            
            # Get or create side folder if enabled
            if self.config['create_side_folders']:
                side_folder_id = self.get_or_create_gdrive_folder(
                    track.side, album_folder_id
                )
                return side_folder_id
            else:
                return album_folder_id
                
        except Exception as e:
            self.logger.error(f"Failed to create Google Drive folder structure: {e}")
            return self.config['gdrive_folder_id']  # Fallback to root folder
            
    def get_or_create_gdrive_folder(self, name: str, parent_id: str) -> str:
        """Get existing folder or create new one in Google Drive"""
        try:
            # Search for existing folder
            query = f"name='{name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.gdrive_service.files().list(q=query).execute()
            items = results.get('files', [])
            
            if items:
                return items[0]['id']
                
            # Create new folder
            folder_metadata = {
                'name': name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            
            folder = self.gdrive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            return folder.get('id')
            
        except Exception as e:
            self.logger.error(f"Failed to get/create Google Drive folder {name}: {e}")
            return parent_id
            
    async def sync_to_databases(self, track: MusicTrack):
        """Sync track data to Airtable and Google Sheets"""
        try:
            # Sync to Airtable
            if self.airtable_client:
                airtable_id = await self.sync_to_airtable(track)
                if airtable_id:
                    track.airtable_id = airtable_id
                    
            # Sync to Google Sheets
            if self.gsheets_client and self.config.get('gsheets_id'):
                sheets_row = await self.sync_to_gsheets(track)
                if sheets_row:
                    track.sheets_row = sheets_row
                    
            self.update_track_in_db(track)
            
        except Exception as e:
            self.logger.error(f"Failed to sync {track.title} to databases: {e}")
            
    async def sync_to_airtable(self, track: MusicTrack) -> Optional[str]:
        """Sync track to Airtable"""
        try:
            record_data = {
                'File ID': track.file_id,
                'Title': track.title,
                'Artist': track.artist,
                'Album': track.album,
                'Year': track.year,
                'Side': track.side,
                'Duration': track.duration,
                'File Size': track.file_size,
                'Upload Date': track.upload_date.isoformat(),
                'Google Drive URL': track.gdrive_url or '',
                'Processing Status': track.processing_status
            }
            
            result = self.airtable_client.create(record_data)
            self.logger.info(f"Synced to Airtable: {track.title}")
            return result['id']
            
        except Exception as e:
            self.logger.error(f"Failed to sync to Airtable {track.title}: {e}")
            return None
            
    async def sync_to_gsheets(self, track: MusicTrack) -> Optional[int]:
        """Sync track to Google Sheets"""
        try:
            sheet = self.gsheets_client.open_by_key(self.config['gsheets_id']).sheet1
            
            # Prepare row data
            row_data = [
                track.file_id,
                track.title,
                track.artist,
                track.album,
                track.year,
                track.side,
                track.duration,
                track.file_size,
                track.upload_date.isoformat(),
                track.gdrive_url or '',
                track.processing_status
            ]
            
            # Append row
            sheet.append_row(row_data)
            
            # Get row number (approximate)
            row_count = len(sheet.get_all_values())
            
            self.logger.info(f"Synced to Google Sheets: {track.title}")
            return row_count
            
        except Exception as e:
            self.logger.error(f"Failed to sync to Google Sheets {track.title}: {e}")
            return None
            
    async def forward_to_private_channel(self, track: MusicTrack):
        """Forward track to private channel"""
        try:
            if not self.config.get('private_channel_id'):
                return
                
            # Get original message
            original_message = await self.telegram_client.get_messages(
                entity=int(track.channel_id),
                ids=track.message_id
            )
            
            if original_message and original_message[0]:
                # Forward to private channel
                await self.telegram_client.forward_messages(
                    entity=int(self.config['private_channel_id']),
                    messages=original_message[0]
                )
                
                track.forwarded_date = datetime.now()
                self.update_track_in_db(track)
                self.logger.info(f"Forwarded to private channel: {track.title}")
                
        except Exception as e:
            self.logger.error(f"Failed to forward {track.title}: {e}")
            
    def save_track_to_db(self, track: MusicTrack):
        """Save track to SQLite database"""
        try:
            cursor = self.db_connection.cursor()
            
            # Insert or replace track
            cursor.execute('''
                INSERT OR REPLACE INTO music_tracks (
                    file_id, title, artist, album, year, side, duration, file_size,
                    file_path, download_url, message_id, channel_id, upload_date,
                    forwarded_date, gdrive_url, onedrive_url, airtable_id, sheets_row,
                    processing_status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                track.file_id, track.title, track.artist, track.album, track.year,
                track.side, track.duration, track.file_size, track.file_path,
                track.download_url, track.message_id, track.channel_id,
                track.upload_date.isoformat() if track.upload_date else None,
                track.forwarded_date.isoformat() if track.forwarded_date else None,
                track.gdrive_url, track.onedrive_url, track.airtable_id, track.sheets_row,
                track.processing_status, datetime.now().isoformat()
            ))
            
            self.db_connection.commit()
            
        except Exception as e:
            self.logger.error(f"Failed to save track to database: {e}")
            
    def update_track_in_db(self, track: MusicTrack):
        """Update existing track in database"""
        self.save_track_to_db(track)  # Using INSERT OR REPLACE
        
    def get_processing_stats(self) -> Dict:
        """Get current processing statistics"""
        runtime = datetime.now() - self.stats['start_time']
        
        # Get database counts
        cursor = self.db_connection.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM music_tracks")
        total_tracks = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM music_tracks WHERE processing_status = 'completed'")
        completed_tracks = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM music_tracks WHERE processing_status = 'failed'")
        failed_tracks = cursor.fetchone()[0]
        
        return {
            'runtime_seconds': int(runtime.total_seconds()),
            'runtime_formatted': str(runtime).split('.')[0],
            'total_tracks': total_tracks,
            'completed_tracks': completed_tracks,
            'failed_tracks': failed_tracks,
            'successful_downloads': self.stats['successful_downloads'],
            'successful_uploads': self.stats['successful_uploads'],
            'failed_operations': self.stats['failed_operations'],
            'completion_percentage': (completed_tracks / total_tracks * 100) if total_tracks > 0 else 0
        }
        
    async def run_automation(self):
        """Main automation loop with FloodWait handling"""
        self.logger.info("Starting Telegram Music Automation System")
        
        while True:
            try:
                # Initialize all clients
                await self.initialize_clients()
                
                # Start complete history processing
                await self.process_channel_history()
                
                # Log final statistics
                final_stats = self.get_processing_stats()
                self.logger.info(f"Automation completed successfully!")
                self.logger.info(f"Final Statistics: {json.dumps(final_stats, indent=2)}")
                break  # Exit loop if no FloodWaitError occurs
                
            except FloodWaitError as e:
                msg = f"[FLOODWAIT] Automation paused for {e.seconds} seconds."
                self.logger.warning(msg)
                logging.info(msg)
                await asyncio.sleep(e.seconds + 5)
            except Exception as e:
                self.logger.error(f"Automation failed: {e}")
                self.logger.error(traceback.format_exc())
                raise
            finally:
                # Cleanup
                if self.telegram_client:
                    await self.telegram_client.disconnect()
                if self.db_connection:
                    self.db_connection.close()
                self.executor.shutdown(wait=True)

async def main():
    """Main entry point"""
    try:
        archiver = TelegramMusicArchiver()
        await archiver.run_automation()
    except KeyboardInterrupt:
        print("\n⚠️  Automation stopped by user")
    except Exception as e:
        print(f"❌ Automation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
