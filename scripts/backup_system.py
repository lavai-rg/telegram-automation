#!/usr/bin/env python3

import os
import shutil
import tarfile
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    filename='/home/user/telegram-music-automation/logs/system/backup.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_backup():
    """Create system backup"""
    backup_dir = '/home/user/telegram-music-automation/backups'
    os.makedirs(backup_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f'telegram_automation_backup_{timestamp}.tar.gz'
    backup_path = os.path.join(backup_dir, backup_filename)
    
    try:
        with tarfile.open(backup_path, 'w:gz') as tar:
            # Backup scripts
            tar.add('/home/user/telegram-music-automation/scripts', arcname='scripts')
            
            # Backup config (excluding sensitive files)
            tar.add('/home/user/telegram-music-automation/config/config.env.template', arcname='config/config.env.template')
            
            # Backup logs (last 7 days)
            tar.add('/home/user/telegram-music-automation/logs', arcname='logs')
            
            # Backup data files
            if os.path.exists('/home/user/telegram-music-automation/data'):
                tar.add('/home/user/telegram-music-automation/data', arcname='data')
        
        logging.info(f"Backup created successfully: {backup_path}")
        
        # Clean old backups (keep last 7)
        backups = sorted([f for f in os.listdir(backup_dir) if f.startswith('telegram_automation_backup_')])
        if len(backups) > 7:
            for old_backup in backups[:-7]:
                old_path = os.path.join(backup_dir, old_backup)
                os.remove(old_path)
                logging.info(f"Removed old backup: {old_path}")
        
    except Exception as e:
        logging.error(f"Backup failed: {e}")

if __name__ == '__main__':
    create_backup()
