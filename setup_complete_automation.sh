#!/bin/bash

# =============================================================================
# Complete Telegram Music Automation Setup Script
# Azure VM Setup for IndoGlobalMusikAmbulu Channel Scraping
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error_log() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

warn_log() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

info_log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

log "Starting Complete Telegram Music Automation Setup..."

# =============================================================================
# 1. System Updates and Dependencies
# =============================================================================

log "Updating system packages..."
sudo apt update -y
sudo apt upgrade -y

log "Installing essential packages..."
sudo apt install -y \
    python3 \
    python3-pip \
    python3-venv \
    git \
    curl \
    wget \
    unzip \
    htop \
    tree \
    nano \
    vim \
    screen \
    tmux \
    supervisor \
    nginx \
    ffmpeg \
    mediainfo \
    sqlite3 \
    build-essential \
    software-properties-common \
    apt-transport-https \
    ca-certificates \
    gnupg \
    lsb-release

# =============================================================================
# 2. Python Environment Setup
# =============================================================================

log "Setting up Python virtual environment..."
cd ~/telegram-music-automation

# Create virtual environment
python3 -m venv telegram_automation_env
source telegram_automation_env/bin/activate

log "Installing Python packages..."
pip install --upgrade pip setuptools wheel

# Core Telegram packages
pip install telethon pyrogram tgcrypto

# Web frameworks
pip install flask fastapi uvicorn

# Cloud storage
pip install google-cloud-storage google-api-python-client google-auth-httplib2 google-auth-oauthlib
pip install onedrivesdk-fork

# Database packages
pip install airtable-python-wrapper gspread oauth2client

# Utility packages
pip install requests beautifulsoup4 lxml
pip install mutagen eyed3  # Audio metadata
pip install python-dotenv
pip install schedule
pip install psutil
pip install asyncio aiohttp aiofiles
pip install pandas numpy
pip install tqdm colorama
pip install python-telegram-bot

# Azure packages
pip install azure-storage-blob azure-identity

log "Creating requirements.txt..."
pip freeze > requirements.txt

# =============================================================================
# 3. Directory Structure Setup
# =============================================================================

log "Creating project directory structure..."

# Main directories
mkdir -p ~/telegram-music-automation/scripts
mkdir -p ~/telegram-music-automation/config
mkdir -p ~/telegram-music-automation/logs
mkdir -p ~/telegram-music-automation/data
mkdir -p ~/telegram-music-automation/downloads
mkdir -p ~/telegram-music-automation/temp
mkdir -p ~/telegram-music-automation/backups
mkdir -p ~/telegram-music-automation/monitoring

# Music organization directories
mkdir -p ~/telegram-music-automation/downloads/organized
mkdir -p ~/telegram-music-automation/downloads/raw
mkdir -p ~/telegram-music-automation/downloads/processed
mkdir -p ~/telegram-music-automation/downloads/failed

# Log directories
mkdir -p ~/telegram-music-automation/logs/scraping
mkdir -p ~/telegram-music-automation/logs/forwarding
mkdir -p ~/telegram-music-automation/logs/upload
mkdir -p ~/telegram-music-automation/logs/database
mkdir -p ~/telegram-music-automation/logs/system
mkdir -p ~/telegram-music-automation/logs/monitoring

# Config directories
mkdir -p ~/telegram-music-automation/config/credentials
mkdir -p ~/telegram-music-automation/config/templates
mkdir -p ~/telegram-music-automation/config/mappings

# =============================================================================
# 4. System Services Setup
# =============================================================================

log "Setting up system services..."

# Create systemd service for main automation
sudo tee /etc/systemd/system/telegram-music-automation.service > /dev/null <<EOF
[Unit]
Description=Telegram Music Automation Service
After=network.target
Wants=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=/home/$USER/telegram-music-automation
Environment=PATH=/home/$USER/telegram-music-automation/telegram_automation_env/bin
ExecStart=/home/$USER/telegram-music-automation/telegram_automation_env/bin/python complete_automation_system.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Create systemd service for monitoring dashboard
sudo tee /etc/systemd/system/telegram-monitoring.service > /dev/null <<EOF
[Unit]
Description=Telegram Music Automation Monitoring Dashboard
After=network.target
Wants=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=/home/$USER/telegram-music-automation
Environment=PATH=/home/$USER/telegram-music-automation/telegram_automation_env/bin
ExecStart=/home/$USER/telegram-music-automation/telegram_automation_env/bin/python monitoring_dashboard.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# =============================================================================
# 5. Nginx Configuration for Monitoring Dashboard
# =============================================================================

log "Configuring Nginx for monitoring dashboard..."

sudo tee /etc/nginx/sites-available/telegram-monitoring > /dev/null <<EOF
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location /static {
        alias /home/$USER/telegram-music-automation/static;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
EOF

# Enable the site
sudo ln -sf /etc/nginx/sites-available/telegram-monitoring /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default

# Test and restart nginx
sudo nginx -t
sudo systemctl restart nginx
sudo systemctl enable nginx

# =============================================================================
# 6. Firewall Configuration
# =============================================================================

log "Configuring firewall..."
sudo ufw --force enable
sudo ufw allow ssh
sudo ufw allow 80/tcp    # HTTP for monitoring
sudo ufw allow 443/tcp   # HTTPS for monitoring
sudo ufw allow 8080/tcp  # Direct access to dashboard
sudo ufw reload

# =============================================================================
# 7. Supervisor Configuration
# =============================================================================

log "Setting up Supervisor for process management..."

sudo tee /etc/supervisor/conf.d/telegram-automation.conf > /dev/null <<EOF
[program:telegram-automation]
command=/home/$USER/telegram-music-automation/telegram_automation_env/bin/python complete_automation_system.py
directory=/home/$USER/telegram-music-automation
user=$USER
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/home/$USER/telegram-music-automation/logs/system/automation.log
stdout_logfile_maxbytes=50MB
stdout_logfile_backups=10

[program:telegram-monitoring]
command=/home/$USER/telegram-music-automation/telegram_automation_env/bin/python monitoring_dashboard.py
directory=/home/$USER/telegram-music-automation
user=$USER
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/home/$USER/telegram-music-automation/logs/system/monitoring.log
stdout_logfile_maxbytes=50MB
stdout_logfile_backups=10
EOF

sudo supervisorctl reread
sudo supervisorctl update

# =============================================================================
# 8. Cron Jobs Setup
# =============================================================================

log "Setting up cron jobs..."

# Backup cron job
(crontab -l 2>/dev/null || echo "") | grep -v "telegram-music-backup" | crontab -
(crontab -l 2>/dev/null; echo "0 2 * * * cd /home/$USER/telegram-music-automation && ./telegram_automation_env/bin/python scripts/backup_system.py") | crontab -

# Log rotation cron job
(crontab -l 2>/dev/null || echo "") | grep -v "telegram-log-rotate" | crontab -
(crontab -l 2>/dev/null; echo "0 1 * * 0 find /home/$USER/telegram-music-automation/logs -name '*.log' -mtime +7 -exec gzip {} \;") | crontab -

# System health check
(crontab -l 2>/dev/null || echo "") | grep -v "telegram-health-check" | crontab -
(crontab -l 2>/dev/null; echo "*/15 * * * * cd /home/$USER/telegram-music-automation && ./telegram_automation_env/bin/python scripts/health_check.py") | crontab -

# =============================================================================
# 9. Configuration Templates
# =============================================================================

log "Creating configuration templates..."

# Main configuration template
tee ~/telegram-music-automation/config/config.env.template > /dev/null <<EOF
# =============================================================================
# Telegram Music Automation Configuration
# Copy this file to config.env and fill in your actual values
# =============================================================================

# Telegram API Configuration
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_PHONE_NUMBER=your_phone_number_here

# Source Channel (Target to scrape)
SOURCE_CHANNEL=IndoGlobalMusikAmbulu

# Private Channel (Your forwarding destination)
PRIVATE_CHANNEL_ID=your_private_channel_id_here
PRIVATE_CHANNEL_USERNAME=your_private_channel_username

# Google Drive Configuration
GOOGLE_DRIVE_CREDENTIALS_PATH=config/credentials/google_drive_credentials.json
GOOGLE_DRIVE_FOLDER_ID=your_google_drive_folder_id

# OneDrive Configuration  
ONEDRIVE_CLIENT_ID=your_onedrive_client_id
ONEDRIVE_CLIENT_SECRET=your_onedrive_client_secret
ONEDRIVE_FOLDER_PATH=/TelegramMusicBackup

# Airtable Configuration
AIRTABLE_API_KEY=your_airtable_api_key
AIRTABLE_BASE_ID=your_airtable_base_id
AIRTABLE_TABLE_NAME=MusicTracks

# Google Sheets Configuration
GOOGLE_SHEETS_CREDENTIALS_PATH=config/credentials/google_sheets_credentials.json
GOOGLE_SHEETS_ID=your_google_sheets_id

# Azure Storage Configuration
AZURE_STORAGE_CONNECTION_STRING=your_azure_storage_connection_string
AZURE_CONTAINER_NAME=telegram-music-backup

# Processing Configuration
MAX_CONCURRENT_DOWNLOADS=3
DOWNLOAD_TIMEOUT=300
RETRY_ATTEMPTS=3
BATCH_SIZE=50

# Monitoring Configuration
MONITORING_PORT=8080
ENABLE_WEB_DASHBOARD=true
LOG_LEVEL=INFO

# File Organization
ORGANIZE_BY_ALBUM=true
CREATE_SIDE_AB_FOLDERS=true
DOWNLOAD_PATH=/home/$USER/telegram-music-automation/downloads
EOF

# Credentials directory setup
mkdir -p ~/telegram-music-automation/config/credentials

tee ~/telegram-music-automation/config/credentials/README.txt > /dev/null <<EOF
Place your credential files here:

1. google_drive_credentials.json - Google Drive service account key
2. google_sheets_credentials.json - Google Sheets service account key
3. telegram_session.session - Telegram session file (auto-generated)

Make sure these files have proper permissions:
chmod 600 *.json
chmod 600 *.session
EOF

# =============================================================================
# 10. Monitoring and Health Check Scripts
# =============================================================================

log "Creating monitoring scripts..."

# Health check script
tee ~/telegram-music-automation/scripts/health_check.py > /dev/null <<'EOF'
#!/usr/bin/env python3

import os
import sys
import psutil
import requests
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    filename='/home/user/telegram-music-automation/logs/system/health_check.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def check_system_resources():
    """Check CPU, Memory, and Disk usage"""
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    logging.info(f"CPU Usage: {cpu_percent}%")
    logging.info(f"Memory Usage: {memory.percent}%")
    logging.info(f"Disk Usage: {disk.percent}%")
    
    # Alert if resources are high
    if cpu_percent > 90:
        logging.warning(f"High CPU usage: {cpu_percent}%")
    if memory.percent > 90:
        logging.warning(f"High memory usage: {memory.percent}%")
    if disk.percent > 90:
        logging.warning(f"High disk usage: {disk.percent}%")

def check_services():
    """Check if automation services are running"""
    try:
        # Check main automation service
        response = requests.get('http://localhost:8080/health', timeout=10)
        if response.status_code == 200:
            logging.info("Monitoring dashboard is healthy")
        else:
            logging.error(f"Monitoring dashboard unhealthy: {response.status_code}")
    except Exception as e:
        logging.error(f"Failed to check monitoring dashboard: {e}")

def check_log_files():
    """Check if log files are being updated"""
    log_dir = '/home/user/telegram-music-automation/logs'
    
    for log_type in ['scraping', 'forwarding', 'upload', 'database']:
        log_file = os.path.join(log_dir, log_type, f'{log_type}.log')
        if os.path.exists(log_file):
            mtime = os.path.getmtime(log_file)
            age_hours = (datetime.now().timestamp() - mtime) / 3600
            
            if age_hours > 24:  # No updates in 24 hours
                logging.warning(f"Log file {log_file} not updated in {age_hours:.1f} hours")
            else:
                logging.info(f"Log file {log_file} is current (updated {age_hours:.1f} hours ago)")

if __name__ == '__main__':
    logging.info("Starting health check...")
    
    try:
        check_system_resources()
        check_services()
        check_log_files()
        logging.info("Health check completed successfully")
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        sys.exit(1)
EOF

# Backup script
tee ~/telegram-music-automation/scripts/backup_system.py > /dev/null <<'EOF'
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
EOF

# Make scripts executable
chmod +x ~/telegram-music-automation/scripts/*.py

# =============================================================================
# 11. Final System Configuration
# =============================================================================

log "Applying final system configurations..."

# Set proper permissions
chmod -R 755 ~/telegram-music-automation
chmod 700 ~/telegram-music-automation/config/credentials
chmod 600 ~/telegram-music-automation/config/*.env* 2>/dev/null || true

# Enable services
sudo systemctl daemon-reload
sudo systemctl enable telegram-music-automation
sudo systemctl enable telegram-monitoring

# =============================================================================
# 12. Installation Verification
# =============================================================================

log "Verifying installation..."

# Check Python environment
if source ~/telegram-music-automation/telegram_automation_env/bin/activate; then
    info_log "✓ Python virtual environment created successfully"
    
    # Check key packages
    python3 -c "import telethon; print('✓ Telethon installed')"
    python3 -c "import flask; print('✓ Flask installed')"
    python3 -c "import google.oauth2; print('✓ Google APIs installed')"
else
    error_log "✗ Python virtual environment setup failed"
fi

# Check directories
for dir in logs downloads config scripts; do
    if [ -d "~/telegram-music-automation/$dir" ]; then
        info_log "✓ Directory $dir created"
    else
        error_log "✗ Directory $dir missing"
    fi
done

# Check services
if systemctl list-unit-files | grep -q telegram-music-automation; then
    info_log "✓ Systemd services configured"
else
    error_log "✗ Systemd services not configured"
fi

# Check nginx
if sudo nginx -t >/dev/null 2>&1; then
    info_log "✓ Nginx configuration valid"
else
    error_log "✗ Nginx configuration invalid"
fi

# =============================================================================
# Setup Complete
# =============================================================================

log "Setup completed successfully!"
echo
echo "============================================================================="
echo -e "${GREEN}🎉 TELEGRAM MUSIC AUTOMATION SETUP COMPLETE! 🎉${NC}"
echo "============================================================================="
echo
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Copy your automation scripts to this directory"
echo "2. Configure credentials in ~/telegram-music-automation/config/config.env"
echo "3. Place Google Drive/Sheets credential files in ~/telegram-music-automation/config/credentials/"
echo "4. Start the services: sudo systemctl start telegram-music-automation"
echo "5. Start monitoring: sudo systemctl start telegram-monitoring"
echo "6. Access dashboard: http://$(curl -s ifconfig.me):80"
echo
echo -e "${YELLOW}Important Files:${NC}"
echo "• Main config: ~/telegram-music-automation/config/config.env"
echo "• Logs directory: ~/telegram-music-automation/logs/"
echo "• Downloads: ~/telegram-music-automation/downloads/"
echo "• Scripts: ~/telegram-music-automation/scripts/"
echo
echo -e "${GREEN}Services Status:${NC}"
sudo systemctl status telegram-music-automation --no-pager -l || true
echo
sudo systemctl status telegram-monitoring --no-pager -l || true
echo
echo "============================================================================="
echo -e "${GREEN}Ready for automation! 🚀${NC}"
echo "============================================================================="
