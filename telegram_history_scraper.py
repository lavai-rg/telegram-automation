#!/usr/bin/env python3
"""
Telegram Music Channel History Scraper
Specialized for scraping historical data from music channels
Focus on bulk extraction of existing posts, not real-time monitoring
"""

import asyncio
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import aiohttp
import aiofiles
from pathlib import Path
import time
import hashlib

# Required libraries
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, DocumentAttributeAudio
from telethon.errors import FloodWaitError, ChannelPrivateError
import pandas as pd
from dataclasses import dataclass

@dataclass
class HistoryConfig:
    """Configuration for history scraping"""
    # Telegram API credentials
    API_ID: str = ""
    API_HASH: str = ""
    PHONE_NUMBER: str = ""
    
    # History scraping parameters
    MAX_MESSAGES: int = 10000  # Maximum messages to scrape
    BATCH_SIZE: int = 100      # Messages per batch
    DELAY_BETWEEN_BATCHES: float = 2.0  # Seconds delay to avoid rate limits
    
    # Date range for history (optional)
    START_DATE: Optional[datetime] = None  # None = from beginning
    END_DATE: Optional[datetime] = None    # None = to latest
    
    # Output configuration
    OUTPUT_DIR: str = "./telegram_history"
    SAVE_RAW_DATA: bool = True
    SAVE_METADATA_JSON: bool = True
    SAVE_METADATA_CSV: bool = True
    
    # Resume capability
    CHECKPOINT_FILE: str = "scraping_checkpoint.json"
    RESUME_FROM_CHECKPOINT: bool = True

class TelegramHistoryScraper:
    def __init__(self, config: HistoryConfig):
        self.config = config
        self.client = None
        self.logger = self._setup_logger()
        self.scraped_count = 0
        self.checkpoint_data = {}
        
    def _setup_logger(self):
        """Setup detailed logging for history scraping"""
        os.makedirs("logs", exist_ok=True)
        
        logger = logging.getLogger("HistoryScraper")
        logger.setLevel(logging.INFO)
        
        # File handler with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fh = logging.FileHandler(f'logs/history_scraper_{timestamp}.log')
        fh.setLevel(logging.INFO)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def load_checkpoint(self) -> Dict:
        """Load scraping checkpoint to resume from last position"""
        if not self.config.RESUME_FROM_CHECKPOINT:
            return {}
            
        checkpoint_path = os.path.join(self.config.OUTPUT_DIR, self.config.CHECKPOINT_FILE)
        
        try:
            if os.path.exists(checkpoint_path):
                with open(checkpoint_path, 'r', encoding='utf-8') as f:
                    checkpoint = json.load(f)
                self.logger.info(f"Loaded checkpoint: {checkpoint.get('last_message_id', 'N/A')}")
                return checkpoint
        except Exception as e:
            self.logger.error(f"Error loading checkpoint: {e}")
        
        return {}
    
    def save_checkpoint(self, last_message_id: int, processed_count: int, channel_username: str):
        """Save current scraping progress"""
        checkpoint = {
            'last_message_id': last_message_id,
            'processed_count': processed_count,
            'channel_username': channel_username,
            'timestamp': datetime.now().isoformat(),
            'config_hash': hashlib.md5(str(self.config.__dict__).encode()).hexdigest()
        }
        
        checkpoint_path = os.path.join(self.config.OUTPUT_DIR, self.config.CHECKPOINT_FILE)
        
        try:
            with open(checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Checkpoint saved: processed {processed_count} messages")
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")
    
    async def initialize_telegram_client(self):
        """Initialize Telegram client with proper session handling"""
        session_file = f"history_session_{hashlib.md5(self.config.PHONE_NUMBER.encode()).hexdigest()[:8]}"
        
        self.client = TelegramClient(
            session_file,
            self.config.API_ID, 
            self.config.API_HASH
        )
        
        await self.client.start(phone=self.config.PHONE_NUMBER)
        
        # Get client info
        me = await self.client.get_me()
        self.logger.info(f"Connected as: {me.username or me.first_name} ({me.id})")
    
    async def get_channel_info(self, channel_username: str) -> Dict:
        """Get detailed channel information"""
        try:
            entity = await self.client.get_entity(channel_username)
            
            # Get participant count if possible
            participants_count = None
            try:
                if hasattr(entity, 'participants_count'):
                    participants_count = entity.participants_count
                else:
                    # Try to get full channel info
                    full_channel = await self.client(GetFullChannelRequest(entity))
                    participants_count = full_channel.full_chat.participants_count
            except:
                pass
            
            info = {
                'id': entity.id,
                'title': entity.title,
                'username': entity.username,
                'participants_count': participants_count,
                'access_hash': getattr(entity, 'access_hash', None),
                'date_created': getattr(entity, 'date', None),
                'description': getattr(entity, 'about', ''),
                'verified': getattr(entity, 'verified', False),
                'restricted': getattr(entity, 'restricted', False)
            }
            
            self.logger.info(f"Channel info: {info['title']} ({info.get('participants_count', 'Unknown')} members)")
            return info
            
        except ChannelPrivateError:
            self.logger.error(f"Channel {channel_username} is private or doesn't exist")
            raise
        except Exception as e:
            self.logger.error(f"Error getting channel info: {e}")
            raise
    
    async def scrape_channel_history(self, channel_username: str) -> List[Dict]:
        """Scrape complete history of a music channel"""
        self.logger.info(f"Starting history scrape for channel: {channel_username}")
        
        # Load checkpoint
        checkpoint = self.load_checkpoint()
        start_from_id = checkpoint.get('last_message_id', 0) if checkpoint else 0
        
        # Get channel entity
        entity = await self.client.get_entity(channel_username)
        channel_info = await self.get_channel_info(channel_username)
        
        # Prepare output directory
        channel_output_dir = os.path.join(self.config.OUTPUT_DIR, self._clean_filename(channel_username))
        os.makedirs(channel_output_dir, exist_ok=True)
        
        # Save channel info
        with open(os.path.join(channel_output_dir, 'channel_info.json'), 'w', encoding='utf-8') as f:
            json.dump(channel_info, f, indent=2, ensure_ascii=False, default=str)
        
        all_posts = []
        processed_count = checkpoint.get('processed_count', 0) if checkpoint else 0
        last_message_id = None
        
        try:
            # Calculate total messages estimate
            self.logger.info("Calculating total messages...")
            async for _ in self.client.iter_messages(entity, limit=1):
                break
            
            self.logger.info(f"Starting from message ID: {start_from_id}")
            
            # Iterator parameters
            iter_params = {
                'entity': entity,
                'limit': self.config.MAX_MESSAGES,
                'reverse': False,  # Start from newest
                'min_id': start_from_id
            }
            
            # Add date filters if specified
            if self.config.START_DATE:
                iter_params['offset_date'] = self.config.START_DATE
            if self.config.END_DATE:
                iter_params['offset_date'] = self.config.END_DATE
            
            batch_count = 0
            batch_posts = []
            
            async for message in self.client.iter_messages(**iter_params):
                try:
                    # Process message
                    post_data = await self._process_history_message(message, channel_username)
                    
                    if post_data:
                        batch_posts.append(post_data)
                        all_posts.append(post_data)
                        processed_count += 1
                        last_message_id = message.id
                        
                        # Log progress every 50 messages
                        if processed_count % 50 == 0:
                            self.logger.info(f"Processed {processed_count} messages (ID: {message.id})")
                    
                    # Save batch and create checkpoint
                    if len(batch_posts) >= self.config.BATCH_SIZE:
                        await self._save_batch_data(batch_posts, channel_output_dir, batch_count)
                        
                        # Save checkpoint
                        self.save_checkpoint(last_message_id, processed_count, channel_username)
                        
                        batch_posts = []
                        batch_count += 1
                        
                        # Rate limiting delay
                        if self.config.DELAY_BETWEEN_BATCHES > 0:
                            await asyncio.sleep(self.config.DELAY_BETWEEN_BATCHES)
                
                except FloodWaitError as e:
                    self.logger.warning(f"Rate limit hit, waiting {e.seconds} seconds...")
                    await asyncio.sleep(e.seconds)
                    continue
                    
                except Exception as e:
                    self.logger.error(f"Error processing message {message.id}: {e}")
                    continue
            
            # Save remaining batch
            if batch_posts:
                await self._save_batch_data(batch_posts, channel_output_dir, batch_count)
            
            # Save final checkpoint
            if last_message_id:
                self.save_checkpoint(last_message_id, processed_count, channel_username)
            
            self.logger.info(f"History scraping completed: {processed_count} total messages processed")
            
            # Save combined data
            await self._save_combined_data(all_posts, channel_output_dir)
            
            return all_posts
            
        except Exception as e:
            self.logger.error(f"Error in history scraping: {e}")
            # Save checkpoint on error
            if last_message_id:
                self.save_checkpoint(last_message_id, processed_count, channel_username)
            raise
    
    async def _process_history_message(self, message, channel_username: str) -> Optional[Dict]:
        """Process individual message from history"""
        try:
            # Skip non-media messages
            if not message.media:
                return None
            
            # Check if it's an audio/document
            if not isinstance(message.media, MessageMediaDocument):
                return None
            
            document = message.media.document
            if not document:
                return None
            
            # Check if it's audio
            is_audio = False
            audio_info = {}
            
            if document.mime_type and ('audio' in document.mime_type):
                is_audio = True
                
                # Extract audio attributes
                for attr in document.attributes:
                    if isinstance(attr, DocumentAttributeAudio):
                        audio_info = {
                            'duration': getattr(attr, 'duration', 0),
                            'title': getattr(attr, 'title', ''),
                            'performer': getattr(attr, 'performer', ''),
                            'voice': getattr(attr, 'voice', False),
                            'waveform': getattr(attr, 'waveform', None)
                        }
                        break
            
            if not is_audio:
                return None
            
            # Parse message text for album info
            text = message.text or message.message or ""
            album_info = self._parse_album_info_advanced(text)
            
            # Extract file information
            file_info = {
                'file_id': document.id,
                'file_name': getattr(document, 'file_name', None) or f"audio_{document.id}",
                'file_size': document.size,
                'mime_type': document.mime_type,
                'access_hash': document.access_hash,
                'dc_id': document.dc_id,
                'file_reference': document.file_reference.hex() if document.file_reference else None
            }
            
            # Message metadata
            message_info = {
                'message_id': message.id,
                'date': message.date.isoformat(),
                'views': getattr(message, 'views', 0),
                'forwards': getattr(message, 'forwards', 0),
                'replies': getattr(message, 'replies', {}).replies if getattr(message, 'replies', None) else 0,
                'edit_date': message.edit_date.isoformat() if message.edit_date else None,
                'grouped_id': message.grouped_id,
                'from_id': getattr(message.from_id, 'user_id', None) if message.from_id else None,
                'channel_username': channel_username
            }
            
            # Combine all data
            post_data = {
                **album_info,
                **audio_info,
                **file_info,
                **message_info,
                'raw_text': text,
                'text_length': len(text),
                'has_media': True,
                'media_type': 'audio',
                'scraped_at': datetime.now().isoformat()
            }
            
            return post_data
            
        except Exception as e:
            self.logger.error(f"Error processing message {message.id}: {e}")
            return None
    
    def _parse_album_info_advanced(self, text: str) -> Dict:
        """Advanced parsing of album information from message text"""
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        album_info = {
            'artist': '',
            'album_name': '',
            'year': '',
            'genre': '',
            'label': '',
            'catalog_number': '',
            'format': '',
            'country': '',
            'description': text,
            'track_list': [],
            'urls': []
        }
        
        # Extract URLs
        import re
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        urls = re.findall(url_pattern, text)
        album_info['urls'] = urls
        
        # Parse main artist - album line
        for line in lines:
            # Look for pattern: Artist - Album
            if ' - ' in line and not line.startswith('http') and not line.startswith('Track'):
                parts = line.split(' - ', 1)
                if len(parts) == 2:
                    potential_artist = parts[0].strip()
                    potential_album = parts[1].strip()
                    
                    # Clean and validate
                    if potential_artist and potential_album:
                        album_info['artist'] = potential_artist
                        album_info['album_name'] = potential_album
                        break
        
        # Extract year (4 digits)
        year_pattern = r'\b(19|20)\d{2}\b'
        year_match = re.search(year_pattern, text)
        if year_match:
            album_info['year'] = year_match.group()
        
        # Extract track listing
        track_patterns = [
            r'^(\d+[\.\-\s]+.+)$',  # 1. Track Name
            r'^([A-Z]\d+[\.\-\s]+.+)$',  # A1. Track Name
            r'^(Side [AB][\s\:].+)$'  # Side A: Track Name
        ]
        
        tracks = []
        for line in lines:
            for pattern in track_patterns:
                if re.match(pattern, line, re.MULTILINE):
                    tracks.append(line.strip())
                    break
        
        album_info['track_list'] = tracks
        
        # Extract additional metadata
        metadata_keywords = {
            'genre': ['genre', 'style', 'género'],
            'label': ['label', 'sello', 'editora'],
            'format': ['format', 'formato', 'vinyl', 'cd', 'cassette', 'digital'],
            'country': ['country', 'país', 'origin']
        }
        
        text_lower = text.lower()
        for key, keywords in metadata_keywords.items():
            for keyword in keywords:
                # Look for pattern: keyword: value
                pattern = rf'{keyword}[\s\:]+([^\n\r]+)'
                match = re.search(pattern, text_lower)
                if match:
                    album_info[key] = match.group(1).strip()
                    break
        
        return album_info
    
    async def _save_batch_data(self, batch_data: List[Dict], output_dir: str, batch_number: int):
        """Save batch data to files"""
        batch_filename = f"batch_{batch_number:04d}"
        
        if self.config.SAVE_RAW_DATA:
            # Save as JSON
            json_path = os.path.join(output_dir, f"{batch_filename}.json")
            async with aiofiles.open(json_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(batch_data, indent=2, ensure_ascii=False, default=str))
        
        if self.config.SAVE_METADATA_CSV:
            # Save as CSV
            csv_path = os.path.join(output_dir, f"{batch_filename}.csv")
            df = pd.DataFrame(batch_data)
            df.to_csv(csv_path, index=False, encoding='utf-8')
        
        self.logger.info(f"Saved batch {batch_number} with {len(batch_data)} records")
    
    async def _save_combined_data(self, all_data: List[Dict], output_dir: str):
        """Save combined data from all batches"""
        if not all_data:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save complete JSON
        if self.config.SAVE_RAW_DATA:
            json_path = os.path.join(output_dir, f"complete_history_{timestamp}.json")
            async with aiofiles.open(json_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(all_data, indent=2, ensure_ascii=False, default=str))
        
        # Save complete CSV
        if self.config.SAVE_METADATA_CSV:
            csv_path = os.path.join(output_dir, f"complete_history_{timestamp}.csv")
            df = pd.DataFrame(all_data)
            df.to_csv(csv_path, index=False, encoding='utf-8')
        
        # Generate summary statistics
        await self._generate_summary_stats(all_data, output_dir, timestamp)
        
        self.logger.info(f"Saved complete dataset: {len(all_data)} records")
    
    async def _generate_summary_stats(self, data: List[Dict], output_dir: str, timestamp: str):
        """Generate summary statistics from scraped data"""
        if not data:
            return
        
        df = pd.DataFrame(data)
        
        stats = {
            'total_records': len(data),
            'date_range': {
                'earliest': df['date'].min(),
                'latest': df['date'].max()
            },
            'artists': {
                'total_unique': df['artist'].nunique(),
                'top_10': df['artist'].value_counts().head(10).to_dict()
            },
            'file_stats': {
                'total_size_gb': round(df['file_size'].sum() / (1024**3), 2),
                'avg_size_mb': round(df['file_size'].mean() / (1024**2), 2),
                'total_duration_hours': round(df['duration'].sum() / 3600, 2),
                'avg_duration_minutes': round(df['duration'].mean() / 60, 2)
            },
            'formats': df['mime_type'].value_counts().to_dict(),
            'yearly_distribution': df['year'].value_counts().sort_index().to_dict(),
            'generated_at': datetime.now().isoformat()
        }
        
        # Save statistics
        stats_path = os.path.join(output_dir, f"summary_stats_{timestamp}.json")
        async with aiofiles.open(stats_path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(stats, indent=2, ensure_ascii=False, default=str))
        
        self.logger.info(f"Generated summary statistics: {stats['total_records']} records analyzed")
    
    def _clean_filename(self, filename: str) -> str:
        """Clean filename from invalid characters"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename.strip()

# Multiple channel history scraper
class MultiChannelHistoryScraper:
    def __init__(self, config: HistoryConfig):
        self.config = config
        self.scraper = TelegramHistoryScraper(config)
        self.logger = logging.getLogger("MultiChannelScraper")
    
    async def scrape_multiple_channels(self, channels: List[str]) -> Dict[str, List[Dict]]:
        """Scrape history from multiple channels"""
        await self.scraper.initialize_telegram_client()
        
        results = {}
        
        for i, channel in enumerate(channels):
            self.logger.info(f"Processing channel {i+1}/{len(channels)}: {channel}")
            
            try:
                channel_data = await self.scraper.scrape_channel_history(channel)
                results[channel] = channel_data
                
                self.logger.info(f"Completed {channel}: {len(channel_data)} records")
                
                # Delay between channels to avoid rate limits
                if i < len(channels) - 1:  # Not the last channel
                    await asyncio.sleep(5)
                    
            except Exception as e:
                self.logger.error(f"Failed to scrape {channel}: {e}")
                results[channel] = []
        
        # Generate combined report
        await self._generate_combined_report(results)
        
        return results
    
    async def _generate_combined_report(self, results: Dict[str, List[Dict]]):
        """Generate combined report for all channels"""
        report_dir = os.path.join(self.config.OUTPUT_DIR, "combined_report")
        os.makedirs(report_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Combine all data
        all_data = []
        for channel, data in results.items():
            for record in data:
                record['source_channel'] = channel
                all_data.append(record)
        
        if all_data:
            # Save combined CSV
            df = pd.DataFrame(all_data)
            combined_csv = os.path.join(report_dir, f"all_channels_history_{timestamp}.csv")
            df.to_csv(combined_csv, index=False, encoding='utf-8')
            
            # Generate cross-channel analysis
            analysis = {
                'total_channels': len(results),
                'total_records': len(all_data),
                'records_per_channel': {k: len(v) for k, v in results.items()},
                'top_artists_across_channels': df['artist'].value_counts().head(20).to_dict(),
                'total_size_gb': round(df['file_size'].sum() / (1024**3), 2),
                'date_range': {
                    'earliest': df['date'].min(),
                    'latest': df['date'].max()
                }
            }
            
            analysis_path = os.path.join(report_dir, f"cross_channel_analysis_{timestamp}.json")
            with open(analysis_path, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, indent=2, ensure_ascii=False, default=str)
        
        self.logger.info(f"Combined report generated: {len(all_data)} total records from {len(results)} channels")

# CLI interface
async def main():
    """Main execution for history scraping"""
    # Configuration
    config = HistoryConfig(
        API_ID="your_api_id",
        API_HASH="your_api_hash",
        PHONE_NUMBER="+your_phone_number",
        MAX_MESSAGES=10000,
        BATCH_SIZE=100,
        DELAY_BETWEEN_BATCHES=2.0,
        OUTPUT_DIR="./telegram_history_output",
        START_DATE=datetime(2020, 1, 1),  # Scrape from 2020
        END_DATE=None  # To present
    )
    
    # Target channels for history scraping
    channels = [
        "pustaka_musik_dan_lagu",
        # Add more channels here
    ]
    
    try:
        if len(channels) == 1:
            # Single channel scraping
            scraper = TelegramHistoryScraper(config)
            await scraper.initialize_telegram_client()
            
            results = await scraper.scrape_channel_history(channels[0])
            print(f"✅ History scraping completed: {len(results)} records from {channels[0]}")
            
        else:
            # Multiple channels scraping
            multi_scraper = MultiChannelHistoryScraper(config)
            results = await multi_scraper.scrape_multiple_channels(channels)
            
            total_records = sum(len(data) for data in results.values())
            print(f"✅ Multi-channel history scraping completed: {total_records} total records from {len(channels)} channels")
    
    except Exception as e:
        print(f"❌ Error in history scraping: {e}")
        logging.exception("History scraping failed")
    
    finally:
        # Cleanup
        if 'scraper' in locals() and scraper.client:
            await scraper.client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())