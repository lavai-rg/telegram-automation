#!/usr/bin/env python3
"""
Recent History Test Runner
Simplified script specifically for Recent History testing
"""

import os
import sys
import asyncio
from datetime import datetime
import json

def print_banner():
    print("🎵 Recent History Test - Telegram Music Scraper")
    print("=" * 60)
    print("Target: pustaka_musik_dan_lagu")
    print("Profile: Recent History (Last 2 years)")
    print("Expected: 1,000-5,000 records | 30-60 minutes")
    print("=" * 60)

def check_env_file():
    """Check if .env file exists and has required credentials"""
    if not os.path.exists('.env'):
        print("❌ .env file not found!")
        print("\n🔧 Creating .env template...")
        
        template = """# Telegram API Credentials (get from https://my.telegram.org)
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_PHONE=+your_phone_number_here

# Optional configurations
NOTIFICATION_CHAT_ID=your_chat_id_here
"""
        
        with open('.env', 'w') as f:
            f.write(template)
        
        print("✅ Created .env template")
        print("\n📝 Please edit .env file with your credentials:")
        print("   1. Get API_ID and API_HASH from https://my.telegram.org")
        print("   2. Enter your phone number with country code (+6281234567890)")
        print("   3. Save the file and run this script again")
        print("\n⚠️  Example:")
        print("   TELEGRAM_API_ID=1234567")
        print("   TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890") 
        print("   TELEGRAM_PHONE=+6281234567890")
        return False
    
    # Load .env file
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        api_id = os.getenv('TELEGRAM_API_ID')
        api_hash = os.getenv('TELEGRAM_API_HASH') 
        phone = os.getenv('TELEGRAM_PHONE')
        
        if not api_id or api_id == 'your_api_id_here':
            print("❌ TELEGRAM_API_ID not configured in .env")
            return False
        if not api_hash or api_hash == 'your_api_hash_here':
            print("❌ TELEGRAM_API_HASH not configured in .env") 
            return False
        if not phone or phone == '+your_phone_number_here':
            print("❌ TELEGRAM_PHONE not configured in .env")
            return False
        
        print("✅ Credentials found in .env file")
        return True
        
    except ImportError:
        print("❌ python-dotenv not installed. Run: pip install python-dotenv")
        return False
    except Exception as e:
        print(f"❌ Error loading .env file: {e}")
        return False

def install_dependencies():
    """Install required packages"""
    packages = [
        'telethon', 'aiohttp', 'aiofiles', 'pandas', 'python-dotenv'
    ]
    
    print("📦 Checking dependencies...")
    
    missing = []
    for package in packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"  ✅ {package}")
        except ImportError:
            missing.append(package)
            print(f"  ❌ {package} - missing")
    
    if missing:
        print(f"\n🔧 Installing missing packages: {' '.join(missing)}")
        import subprocess
        try:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install"] + missing, 
                capture_output=True
            )
            print("✅ Dependencies installed successfully!")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install dependencies: {e}")
            print("Please run manually: pip install " + " ".join(missing))
            return False
    
    return True

async def test_connection():
    """Test Telegram connection"""
    print("🔌 Testing Telegram connection...")
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        from telethon import TelegramClient
        
        api_id = os.getenv('TELEGRAM_API_ID')
        api_hash = os.getenv('TELEGRAM_API_HASH')
        phone = os.getenv('TELEGRAM_PHONE')
        
        client = TelegramClient('test_session', api_id, api_hash)
        await client.start(phone=phone)
        
        me = await client.get_me()
        print(f"✅ Connected as: {me.first_name} (@{me.username or 'no_username'})")
        
        # Test channel access
        try:
            entity = await client.get_entity('pustaka_musik_dan_lagu')
            print(f"✅ Can access channel: {entity.title}")
            print(f"   Participants: {getattr(entity, 'participants_count', 'Unknown')}")
            
            # Test message reading
            count = 0
            async for message in client.iter_messages(entity, limit=3):
                count += 1
            print(f"✅ Can read messages: {count} recent messages found")
            
        except Exception as e:
            print(f"❌ Channel access error: {e}")
            print("   Make sure you're a member of @pustaka_musik_dan_lagu")
            return False
        
        await client.disconnect()
        
        # Clean up test session
        if os.path.exists('test_session.session'):
            os.remove('test_session.session')
        
        return True
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

async def run_recent_history():
    """Run the actual Recent History scraping"""
    print("\n🚀 Starting Recent History scraping...")
    print("📊 Target: Last 2 years from pustaka_musik_dan_lagu")
    print("⏱️  Estimated time: 30-60 minutes")
    print("💡 You can interrupt with Ctrl+C and resume later")
    
    try:
        from telegram_history_scraper import TelegramHistoryScraper, HistoryConfig
        from dotenv import load_dotenv
        from datetime import timedelta
        
        load_dotenv()
        
        # Configure for Recent History
        config = HistoryConfig(
            API_ID=os.getenv('TELEGRAM_API_ID'),
            API_HASH=os.getenv('TELEGRAM_API_HASH'),
            PHONE_NUMBER=os.getenv('TELEGRAM_PHONE'),
            
            MAX_MESSAGES=5000,  # Recent History limit
            BATCH_SIZE=150,     # Recent History batch size
            DELAY_BETWEEN_BATCHES=2.0,  # Recent History delay
            
            START_DATE=datetime.now() - timedelta(days=730),  # 2 years ago
            END_DATE=None,  # Until present
            
            OUTPUT_DIR=f"./recent_history_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            SAVE_RAW_DATA=True,
            SAVE_METADATA_JSON=True, 
            SAVE_METADATA_CSV=True,
            
            RESUME_FROM_CHECKPOINT=True
        )
        
        print(f"📁 Output directory: {config.OUTPUT_DIR}")
        print(f"📅 Date range: {config.START_DATE.strftime('%Y-%m-%d')} to present")
        
        # Initialize scraper
        scraper = TelegramHistoryScraper(config)
        await scraper.initialize_telegram_client()
        
        # Start scraping
        start_time = datetime.now()
        print(f"⏰ Started at: {start_time.strftime('%H:%M:%S')}")
        print("-" * 60)
        
        results = await scraper.scrape_channel_history('pustaka_musik_dan_lagu')
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("-" * 60)
        print("🎉 RECENT HISTORY TEST COMPLETED!")
        print(f"⏱️  Duration: {duration}")
        print(f"📊 Records found: {len(results)}")
        print(f"📁 Output saved to: {config.OUTPUT_DIR}")
        
        if results:
            # Quick statistics
            unique_artists = len(set(r.get('artist', '') for r in results if r.get('artist')))
            total_size = sum(r.get('file_size', 0) for r in results)
            total_duration = sum(r.get('duration', 0) for r in results)
            
            print(f"👨‍🎤 Unique artists: {unique_artists}")
            print(f"💾 Total size: {total_size / (1024**3):.2f} GB")
            print(f"🎵 Total duration: {total_duration / 3600:.1f} hours")
            
            # Date range
            dates = [r.get('date') for r in results if r.get('date')]
            if dates:
                print(f"📅 Date range: {min(dates)[:10]} to {max(dates)[:10]}")
            
            # Top artists preview
            artist_counts = {}
            for r in results:
                artist = r.get('artist', '').strip()
                if artist:
                    artist_counts[artist] = artist_counts.get(artist, 0) + 1
            
            if artist_counts:
                top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                print(f"\n🏆 Top 5 Artists:")
                for artist, count in top_artists:
                    print(f"   {artist}: {count} tracks")
        
        print("\n✅ Test completed successfully!")
        print("📈 Next steps:")
        print("   1. Review output files")
        print("   2. If satisfied, run Complete Archive:")
        print("      python run_history_scraping.py scrape -c pustaka_musik_dan_lagu -p complete")
        
        return True
        
    except KeyboardInterrupt:
        print("\n⏹️  Scraping interrupted by user")
        print("💡 You can resume by running this script again")
        return False
    except Exception as e:
        print(f"\n❌ Error during scraping: {e}")
        print("💡 Check your connection and try again")
        return False
    finally:
        if 'scraper' in locals() and scraper.client:
            await scraper.client.disconnect()

async def main():
    """Main execution flow"""
    print_banner()
    
    # Step 1: Check dependencies
    if not install_dependencies():
        return False
    
    # Step 2: Check credentials
    if not check_env_file():
        return False
    
    # Step 3: Test connection
    if not await test_connection():
        return False
    
    # Step 4: Confirm start
    print("\n" + "=" * 60)
    print("🎯 READY TO START RECENT HISTORY TEST")
    print("=" * 60)
    print("Target Channel: pustaka_musik_dan_lagu")  
    print("Profile: Recent History (last 2 years)")
    print("Expected Duration: 30-60 minutes")
    print("Expected Records: 1,000-5,000")
    
    confirm = input("\nProceed with Recent History test? (Y/n): ").strip().lower()
    if confirm in ['', 'y', 'yes']:
        success = await run_recent_history()
        return success
    else:
        print("Test cancelled.")
        return False

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        if result:
            print("\n🎉 Recent History test completed successfully!")
        else:
            print("\n❌ Recent History test failed or was cancelled")
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted")
        sys.exit(1)