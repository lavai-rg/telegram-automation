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
