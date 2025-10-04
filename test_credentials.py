import asyncio
from telethon import TelegramClient

async def test_all_credentials():
    # Test Telegram authentication
    client = TelegramClient('test_session', 29138494, '2d1c6319f940d49361c87582af638261')
    
    try:
        await client.start(phone='+628974678209')
        print("✅ Telegram authentication SUCCESS")
        
        # Test bot token
        bot_client = TelegramClient('test_bot', 29138494, '2d1c6319f940d49361c87582af638261')
        await bot_client.start(bot_token='8306773528:AAEJ8CiKcrGWFaBmmefAkjxpWty4A88vJwE')
        bot_me = await bot_client.get_me()
        print(f"✅ Bot authentication SUCCESS: @{bot_me.username}")
        
        # Test private channel access
        private_channel = await client.get_entity(-1002943538191)
        print(f"✅ Private channel access SUCCESS: {private_channel.title}")
        
        # Test source channel
        source_channel = await client.get_entity('IndoGlobalMusikAmbulu')
        print(f"✅ Source channel access SUCCESS: {source_channel.title}")
        
        return True
        
    except Exception as e:
        print(f"❌ Credential test FAILED: {e}")
        return False
    finally:
        await client.disconnect()
        if 'bot_client' in locals():
            await bot_client.disconnect()

asyncio.run(test_all_credentials())
