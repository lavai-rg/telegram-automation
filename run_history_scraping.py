#!/usr/bin/env python3
"""
Simple Runner Script for Telegram History Scraping
Easy-to-use interface for running history scraping with different profiles
"""

import asyncio
import argparse
import sys
import os
from datetime import datetime
import json

# Import our modules
from telegram_history_scraper import TelegramHistoryScraper, MultiChannelHistoryScraper, HistoryConfig
from history_config import HistoryScrapingManager, HistoryDataAnalyzer, print_available_profiles

def load_credentials_from_env():
    """Load credentials from environment variables or .env file"""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    return {
        'api_id': os.getenv('TELEGRAM_API_ID', ''),
        'api_hash': os.getenv('TELEGRAM_API_HASH', ''),
        'phone_number': os.getenv('TELEGRAM_PHONE', '')
    }

def setup_history_config(profile_name: str, credentials: dict, custom_options: dict = None) -> HistoryConfig:
    """Setup history configuration based on profile"""
    
    manager = HistoryScrapingManager()
    profile = manager.get_profile_by_name(profile_name)
    
    # Apply custom options if provided
    if custom_options:
        profile.update(custom_options)
    
    config = HistoryConfig(
        API_ID=credentials['api_id'],
        API_HASH=credentials['api_hash'],
        PHONE_NUMBER=credentials['phone_number'],
        
        MAX_MESSAGES=profile.get('max_messages', 10000),
        BATCH_SIZE=profile.get('batch_size', 100),
        DELAY_BETWEEN_BATCHES=profile.get('delay_between_batches', 2.0),
        
        START_DATE=profile.get('start_date'),
        END_DATE=profile.get('end_date'),
        
        OUTPUT_DIR=f"./history_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        SAVE_RAW_DATA=profile.get('save_raw_data', True),
        SAVE_METADATA_JSON=profile.get('save_metadata_json', True),
        SAVE_METADATA_CSV=profile.get('save_metadata_csv', True),
        
        RESUME_FROM_CHECKPOINT=True
    )
    
    return config

async def scrape_single_channel(channel_username: str, profile_name: str, credentials: dict, custom_options: dict = None):
    """Scrape single channel history"""
    
    print(f"🎵 Starting history scraping for channel: @{channel_username}")
    print(f"📋 Using profile: {profile_name}")
    
    # Setup configuration
    config = setup_history_config(profile_name, credentials, custom_options)
    
    print(f"📁 Output directory: {config.OUTPUT_DIR}")
    print(f"📊 Max messages: {config.MAX_MESSAGES or 'Unlimited'}")
    print(f"⏱️ Batch size: {config.BATCH_SIZE}")
    print(f"🔄 Delay between batches: {config.DELAY_BETWEEN_BATCHES}s")
    
    if config.START_DATE:
        print(f"📅 Start date: {config.START_DATE.strftime('%Y-%m-%d')}")
    if config.END_DATE:
        print(f"📅 End date: {config.END_DATE.strftime('%Y-%m-%d')}")
    
    print("\n" + "="*60)
    
    try:
        # Initialize scraper
        scraper = TelegramHistoryScraper(config)
        await scraper.initialize_telegram_client()
        
        # Start scraping
        start_time = datetime.now()
        results = await scraper.scrape_channel_history(channel_username)
        end_time = datetime.now()
        
        duration = end_time - start_time
        
        print("\n" + "="*60)
        print("✅ SCRAPING COMPLETED!")
        print(f"📊 Total records: {len(results)}")
        print(f"⏱️ Duration: {duration}")
        print(f"📁 Output saved to: {config.OUTPUT_DIR}")
        
        # Generate quick stats
        if results:
            total_size = sum(r.get('file_size', 0) for r in results)
            total_duration = sum(r.get('duration', 0) for r in results)
            unique_artists = len(set(r.get('artist', '') for r in results if r.get('artist')))
            
            print(f"👨‍🎤 Unique artists: {unique_artists}")
            print(f"💾 Total size: {total_size / (1024**3):.2f} GB")
            print(f"🎵 Total duration: {total_duration / 3600:.2f} hours")
        
        return results
        
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        raise
    
    finally:
        if scraper.client:
            await scraper.client.disconnect()

async def scrape_multiple_channels(channels: list, profile_name: str, credentials: dict, custom_options: dict = None):
    """Scrape multiple channels"""
    
    print(f"🎵 Starting multi-channel history scraping")
    print(f"📋 Channels: {', '.join(['@' + ch for ch in channels])}")
    print(f"📋 Using profile: {profile_name}")
    
    # Setup configuration
    config = setup_history_config(profile_name, credentials, custom_options)
    
    print(f"📁 Output directory: {config.OUTPUT_DIR}")
    print(f"📊 Max messages per channel: {config.MAX_MESSAGES or 'Unlimited'}")
    
    print("\n" + "="*60)
    
    try:
        # Initialize multi-scraper
        multi_scraper = MultiChannelHistoryScraper(config)
        
        # Start scraping
        start_time = datetime.now()
        results = await multi_scraper.scrape_multiple_channels(channels)
        end_time = datetime.now()
        
        duration = end_time - start_time
        total_records = sum(len(data) for data in results.values())
        
        print("\n" + "="*60)
        print("✅ MULTI-CHANNEL SCRAPING COMPLETED!")
        print(f"📊 Total channels: {len(channels)}")
        print(f"📊 Total records: {total_records}")
        print(f"⏱️ Duration: {duration}")
        print(f"📁 Output saved to: {config.OUTPUT_DIR}")
        
        # Per-channel stats
        print("\n📋 Per-Channel Results:")
        for channel, data in results.items():
            print(f"  @{channel}: {len(data)} records")
        
        return results
        
    except Exception as e:
        print(f"❌ Error during multi-channel scraping: {e}")
        raise

def analyze_existing_data(data_path: str, output_path: str = None):
    """Analyze existing scraped data"""
    
    print(f"📊 Analyzing data from: {data_path}")
    
    try:
        analyzer = HistoryDataAnalyzer(data_path)
        data = analyzer.load_data()
        
        if not data:
            print("❌ No data found or unable to load data")
            return
        
        print(f"📈 Loaded {len(data)} records for analysis")
        
        # Generate analysis report
        if not output_path:
            output_path = f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        report = analyzer.generate_analysis_report(output_path)
        
        # Print summary
        print("\n" + "="*60)
        print("📊 ANALYSIS SUMMARY")
        print("="*60)
        
        summary = report['summary']
        print(f"Total Records: {summary['total_records']}")
        print(f"Unique Artists: {summary['unique_artists']}")
        print(f"Total Size: {summary['total_size_gb']} GB")
        print(f"Total Duration: {summary['total_duration_hours']} hours")
        print(f"Date Range: {summary['date_range']['earliest']} to {summary['date_range']['latest']}")
        
        # Top patterns
        if 'content_patterns' in report:
            content = report['content_patterns']
            
            if content.get('top_artists'):
                print(f"\nTop 5 Artists:")
                for artist, count in list(content['top_artists'].items())[:5]:
                    print(f"  {artist}: {count} tracks")
            
            if content.get('year_distribution'):
                print(f"\nYear Distribution (Top 5):")
                years = sorted(content['year_distribution'].items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0, reverse=True)[:5]
                for year, count in years:
                    print(f"  {year}: {count} tracks")
        
        # Recommendations
        if report.get('recommendations'):
            print(f"\n💡 Recommendations:")
            for i, rec in enumerate(report['recommendations'], 1):
                print(f"  {i}. {rec}")
        
        print(f"\n📄 Full analysis report saved to: {output_path}")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        raise

def main():
    """Main CLI interface"""
    
    parser = argparse.ArgumentParser(description='Telegram Music History Scraper')
    parser.add_argument('action', choices=['scrape', 'multi-scrape', 'analyze', 'profiles'], 
                       help='Action to perform')
    
    # Scraping arguments
    parser.add_argument('-c', '--channel', type=str, help='Channel username (without @)')
    parser.add_argument('-C', '--channels', type=str, nargs='+', help='Multiple channel usernames')
    parser.add_argument('-p', '--profile', type=str, default='recent', 
                       choices=['complete', 'recent', 'vintage', 'metadata', 'sample'],
                       help='Scraping profile to use')
    
    # Custom options
    parser.add_argument('--max-messages', type=int, help='Maximum messages to scrape')
    parser.add_argument('--batch-size', type=int, help='Batch size for processing')
    parser.add_argument('--delay', type=float, help='Delay between batches (seconds)')
    
    # Analysis arguments
    parser.add_argument('-d', '--data-path', type=str, help='Path to data file for analysis')
    parser.add_argument('-o', '--output', type=str, help='Output path for analysis report')
    
    # Credentials
    parser.add_argument('--api-id', type=str, help='Telegram API ID')
    parser.add_argument('--api-hash', type=str, help='Telegram API Hash')
    parser.add_argument('--phone', type=str, help='Phone number')
    
    args = parser.parse_args()
    
    # Handle profiles action
    if args.action == 'profiles':
        print_available_profiles()
        return
    
    # Handle analyze action
    if args.action == 'analyze':
        if not args.data_path:
            print("❌ Data path required for analysis. Use -d/--data-path")
            sys.exit(1)
        
        analyze_existing_data(args.data_path, args.output)
        return
    
    # Load credentials
    credentials = load_credentials_from_env()
    
    # Override with command line arguments if provided
    if args.api_id:
        credentials['api_id'] = args.api_id
    if args.api_hash:
        credentials['api_hash'] = args.api_hash
    if args.phone:
        credentials['phone_number'] = args.phone
    
    # Validate credentials
    if not all([credentials['api_id'], credentials['api_hash'], credentials['phone_number']]):
        print("❌ Missing credentials. Please provide:")
        print("  - TELEGRAM_API_ID environment variable or --api-id")
        print("  - TELEGRAM_API_HASH environment variable or --api-hash")
        print("  - TELEGRAM_PHONE environment variable or --phone")
        sys.exit(1)
    
    # Prepare custom options
    custom_options = {}
    if args.max_messages:
        custom_options['max_messages'] = args.max_messages
    if args.batch_size:
        custom_options['batch_size'] = args.batch_size
    if args.delay:
        custom_options['delay_between_batches'] = args.delay
    
    # Handle scraping actions
    if args.action == 'scrape':
        if not args.channel:
            print("❌ Channel username required for scraping. Use -c/--channel")
            sys.exit(1)
        
        asyncio.run(scrape_single_channel(args.channel, args.profile, credentials, custom_options))
    
    elif args.action == 'multi-scrape':
        if not args.channels:
            print("❌ Channel usernames required for multi-scraping. Use -C/--channels")
            sys.exit(1)
        
        asyncio.run(scrape_multiple_channels(args.channels, args.profile, credentials, custom_options))

if __name__ == "__main__":
    print("🎵 Telegram Music History Scraper")
    print("=" * 50)
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️  Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)