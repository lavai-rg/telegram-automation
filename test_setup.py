#!/usr/bin/env python3
"""
Quick Test Setup Script
Validates environment and runs initial test scraping
"""

import os
import sys
import subprocess
import asyncio
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = [
        'telethon', 'aiohttp', 'aiofiles', 'pandas', 'python-dotenv'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package} - installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} - missing")
    
    if missing_packages:
        print(f"\n🔧 Installing missing packages: {' '.join(missing_packages)}")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_packages)
            print("✅ All dependencies installed successfully!")
        except subprocess.CalledProcessError:
            print("❌ Failed to install dependencies. Please run manually:")
            print(f"pip install {' '.join(missing_packages)}")
            return False
    
    return True

def check_credentials():
    """Check if credentials are properly configured"""
    
    # Check .env file
    env_file = Path('.env')
    if not env_file.exists():
        print("❌ .env file not found")
        create_env_template()
        return False
    
    # Load and check environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("❌ python-dotenv not installed")
        return False
    
    required_vars = ['TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'TELEGRAM_PHONE']
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if not value or value == 'your_' + var.lower() + '_here':
            missing_vars.append(var)
            print(f"❌ {var} - not configured")
        else:
            # Mask sensitive info
            masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:] if len(value) > 8 else '*' * len(value)
            print(f"✅ {var} - configured ({masked_value})")
    
    if missing_vars:
        print(f"\n🔧 Please configure these variables in .env file:")
        for var in missing_vars:
            print(f"   {var}=your_value_here")
        print("\nGet API credentials from: https://my.telegram.org")
        return False
    
    return True

def create_env_template():
    """Create .env template file"""
    template = """# Telegram API Credentials (get from https://my.telegram.org)
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_PHONE=+your_phone_number_here

# Optional - for notifications
NOTIFICATION_CHAT_ID=your_telegram_chat_id
"""
    
    with open('.env', 'w') as f:
        f.write(template)
    
    print("✅ Created .env template file")
    print("📝 Please edit .env file with your credentials")

async def test_telegram_connection():
    """Test Telegram API connection"""
    print("\n🔌 Testing Telegram connection...")
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        from telethon import TelegramClient
        
        api_id = os.getenv('TELEGRAM_API_ID')
        api_hash = os.getenv('TELEGRAM_API_HASH')
        phone = os.getenv('TELEGRAM_PHONE')
        
        client = TelegramClient('test_session', api_id, api_hash)
        
        await client.start(phone=phone)
        
        # Get client info
        me = await client.get_me()
        print(f"✅ Connected successfully as: {me.first_name} (@{me.username or 'no_username'})")
        
        # Test channel access
        try:
            entity = await client.get_entity('pustaka_musik_dan_lagu')
            print(f"✅ Can access channel: {entity.title}")
            
            # Get a few recent messages to test
            message_count = 0
            async for message in client.iter_messages(entity, limit=5):
                message_count += 1
            
            print(f"✅ Can read messages: {message_count} recent messages found")
            
        except Exception as e:
            print(f"⚠️  Channel access issue: {e}")
            print("   - Make sure you're a member of the channel")
            print("   - Check if channel username is correct")
        
        await client.disconnect()
        
        # Clean up test session
        if os.path.exists('test_session.session'):
            os.remove('test_session.session')
        
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def run_sample_test():
    """Run sample scraping test"""
    print("\n🧪 Running sample test scraping...")
    
    try:
        result = subprocess.run([
            sys.executable, 'run_history_scraping.py', 
            'scrape', '-c', 'pustaka_musik_dan_lagu', '-p', 'sample'
        ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
        
        if result.returncode == 0:
            print("✅ Sample test completed successfully!")
            print("\n📊 Output preview:")
            print(result.stdout[-500:])  # Last 500 characters
            return True
        else:
            print("❌ Sample test failed:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("⏰ Sample test timed out (took longer than 5 minutes)")
        return False
    except Exception as e:
        print(f"❌ Error running sample test: {e}")
        return False

def main():
    """Main setup and test function"""
    print("🎵 Telegram Music History Scraper - Setup & Test")
    print("=" * 60)
    
    # Step 1: Check dependencies
    print("\n📦 Step 1: Checking dependencies...")
    if not check_dependencies():
        print("❌ Setup failed at dependency check")
        return False
    
    # Step 2: Check credentials
    print("\n🔐 Step 2: Checking credentials...")
    if not check_credentials():
        print("❌ Setup failed at credentials check")
        return False
    
    # Step 3: Test connection
    print("\n🔌 Step 3: Testing Telegram connection...")
    try:
        connection_ok = asyncio.run(test_telegram_connection())
        if not connection_ok:
            print("❌ Setup failed at connection test")
            return False
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False
    
    # Step 4: Run sample test
    print("\n🧪 Step 4: Running sample scraping test...")
    if not run_sample_test():
        print("❌ Setup failed at sample test")
        return False
    
    # Success!
    print("\n" + "=" * 60)
    print("🎉 SETUP & TEST COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    
    print("\n🚀 Next Steps:")
    print("1. Run Recent History test:")
    print("   python run_history_scraping.py scrape -c pustaka_musik_dan_lagu -p recent")
    print("\n2. If satisfied, run Complete Archive:")
    print("   python run_history_scraping.py scrape -c pustaka_musik_dan_lagu -p complete")
    print("\n3. Analyze results:")
    print("   python run_history_scraping.py analyze -d history_output_*/pustaka_musik_dan_lagu/complete_history_*.json")
    
    # Show available profiles
    print("\n📋 Available profiles:")
    try:
        result = subprocess.run([
            sys.executable, 'run_history_scraping.py', 'profiles'
        ], capture_output=True, text=True)
        if result.returncode == 0:
            print(result.stdout)
    except:
        pass
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⏹️  Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Setup failed with error: {e}")
        sys.exit(1)