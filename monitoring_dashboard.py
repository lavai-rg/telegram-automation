#!/usr/bin/env python3

"""
Telegram Music Automation Monitoring Dashboard
Real-time monitoring and control interface for the automation system
"""

from flask import Flask, render_template, jsonify, request, send_from_directory
import sqlite3
import json
import os
import psutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
import threading
import time
from typing import Dict, List, Any
import subprocess

class AutomationMonitor:
    """Monitoring dashboard for Telegram music automation"""
    
    def __init__(self):
        self.app = Flask(__name__)
        self.setup_logging()
        self.setup_routes()
        
        # Database connection
        self.db_path = 'data/music_tracks.db'
        
        # System monitoring
        self.system_stats = {}
        self.update_system_stats()
        
        # Start background monitoring thread
        self.monitoring_thread = threading.Thread(target=self.background_monitoring, daemon=True)
        self.monitoring_thread.start()
        
    def setup_logging(self):
        """Setup logging for dashboard"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/monitoring/dashboard.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('MonitoringDashboard')
        
    def setup_routes(self):
        """Setup Flask routes"""
        
        @self.app.route('/')
        def dashboard():
            return self.render_dashboard()
            
        @self.app.route('/api/stats')
        def api_stats():
            return jsonify(self.get_processing_stats())
            
        @self.app.route('/api/tracks')
        def api_tracks():
            page = int(request.args.get('page', 1))
            limit = int(request.args.get('limit', 50))
            status = request.args.get('status', 'all')
            
            return jsonify(self.get_tracks(page, limit, status))
            
        @self.app.route('/api/system')
        def api_system():
            return jsonify(self.system_stats)
            
        @self.app.route('/api/logs')
        def api_logs():
            log_type = request.args.get('type', 'main')
            lines = int(request.args.get('lines', 100))
            
            return jsonify(self.get_log_content(log_type, lines))
            
        @self.app.route('/api/control/<action>')
        def api_control(action):
            return jsonify(self.handle_control_action(action))
            
        @self.app.route('/health')
        def health_check():
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'uptime': self.get_uptime()
            })
            
        @self.app.route('/static/<path:filename>')
        def static_files(filename):
            return send_from_directory('static', filename)
            
    def render_dashboard(self):
        """Render main dashboard HTML"""
        html_template = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Telegram Music Automation Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 30px;
            color: white;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .header p {
            font-size: 1.2em;
            opacity: 0.9;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-card {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
            border-left: 4px solid #667eea;
            transition: transform 0.2s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-2px);
        }
        
        .stat-card h3 {
            color: #667eea;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-bottom: 10px;
            font-weight: 600;
        }
        
        .stat-card .value {
            font-size: 2.2em;
            font-weight: 700;
            color: #333;
            margin-bottom: 5px;
        }
        
        .stat-card .label {
            color: #666;
            font-size: 0.95em;
        }
        
        .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
        }
        
        .status-running { background-color: #10B981; }
        .status-stopped { background-color: #EF4444; }
        .status-warning { background-color: #F59E0B; }
        
        .main-content {
            display: grid;
            grid-template-columns: 1fr 350px;
            gap: 30px;
        }
        
        .left-panel {
            display: flex;
            flex-direction: column;
            gap: 25px;
        }
        
        .right-panel {
            display: flex;
            flex-direction: column;
            gap: 25px;
        }
        
        .panel {
            background: white;
            border-radius: 12px;
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        .panel-header {
            background: linear-gradient(135deg, #667eea, #764ba2);
            color: white;
            padding: 20px 25px;
            font-weight: 600;
        }
        
        .panel-content {
            padding: 25px;
        }
        
        .chart-container {
            height: 300px;
            margin-bottom: 20px;
        }
        
        .tracks-table {
            width: 100%;
            border-collapse: collapse;
        }
        
        .tracks-table th,
        .tracks-table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e5e7eb;
        }
        
        .tracks-table th {
            background: #f9fafb;
            font-weight: 600;
            color: #374151;
        }
        
        .status-badge {
            padding: 4px 8px;
            border-radius: 6px;
            font-size: 0.8em;
            font-weight: 500;
        }
        
        .status-completed { background: #d1fae5; color: #065f46; }
        .status-processing { background: #fef3c7; color: #92400e; }
        .status-failed { background: #fee2e2; color: #991b1b; }
        .status-pending { background: #e0e7ff; color: #3730a3; }
        
        .control-buttons {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }
        
        .btn {
            padding: 10px 16px;
            border: none;
            border-radius: 8px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s ease;
        }
        
        .btn-primary {
            background: #667eea;
            color: white;
        }
        
        .btn-primary:hover {
            background: #5a67d8;
        }
        
        .btn-danger {
            background: #ef4444;
            color: white;
        }
        
        .btn-danger:hover {
            background: #dc2626;
        }
        
        .btn-warning {
            background: #f59e0b;
            color: white;
        }
        
        .btn-warning:hover {
            background: #d97706;
        }
        
        .log-container {
            background: #1f2937;
            color: #e5e7eb;
            padding: 15px;
            border-radius: 8px;
            font-family: 'Courier New', monospace;
            font-size: 0.85em;
            max-height: 300px;
            overflow-y: auto;
            line-height: 1.4;
        }
        
        .progress-bar {
            width: 100%;
            height: 8px;
            background: #e5e7eb;
            border-radius: 4px;
            overflow: hidden;
            margin-bottom: 10px;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea, #764ba2);
            border-radius: 4px;
            transition: width 0.3s ease;
        }
        
        .system-metric {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 0;
            border-bottom: 1px solid #f3f4f6;
        }
        
        .system-metric:last-child {
            border-bottom: none;
        }
        
        .metric-label {
            font-weight: 500;
            color: #374151;
        }
        
        .metric-value {
            font-weight: 600;
            color: #667eea;
        }
        
        @media (max-width: 768px) {
            .main-content {
                grid-template-columns: 1fr;
            }
            
            .stats-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎵 Telegram Music Automation</h1>
            <p>@IndoGlobalMusikAmbulu Archive Processing Dashboard</p>
        </div>
        
        <div class="stats-grid" id="statsGrid">
            <!-- Stats will be populated by JavaScript -->
        </div>
        
        <div class="main-content">
            <div class="left-panel">
                <div class="panel">
                    <div class="panel-header">
                        <h3>Processing Progress</h3>
                    </div>
                    <div class="panel-content">
                        <div class="progress-bar">
                            <div class="progress-fill" id="progressBar" style="width: 0%"></div>
                        </div>
                        <div class="chart-container">
                            <canvas id="progressChart"></canvas>
                        </div>
                    </div>
                </div>
                
                <div class="panel">
                    <div class="panel-header">
                        <h3>Recent Tracks</h3>
                    </div>
                    <div class="panel-content">
                        <table class="tracks-table">
                            <thead>
                                <tr>
                                    <th>Title</th>
                                    <th>Artist</th>
                                    <th>Status</th>
                                    <th>Time</th>
                                </tr>
                            </thead>
                            <tbody id="recentTracks">
                                <!-- Tracks will be populated by JavaScript -->
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
            
            <div class="right-panel">
                <div class="panel">
                    <div class="panel-header">
                        <h3>System Control</h3>
                    </div>
                    <div class="panel-content">
                        <div class="control-buttons">
                            <button class="btn btn-primary" onclick="controlAction('start')">▶️ Start</button>
                            <button class="btn btn-warning" onclick="controlAction('pause')">⏸️ Pause</button>
                            <button class="btn btn-danger" onclick="controlAction('stop')">⏹️ Stop</button>
                        </div>
                        <div id="controlStatus">
                            <span class="status-indicator status-running"></span>
                            Automation Running
                        </div>
                    </div>
                </div>
                
                <div class="panel">
                    <div class="panel-header">
                        <h3>System Resources</h3>
                    </div>
                    <div class="panel-content" id="systemResources">
                        <!-- System stats will be populated by JavaScript -->
                    </div>
                </div>
                
                <div class="panel">
                    <div class="panel-header">
                        <h3>Recent Logs</h3>
                    </div>
                    <div class="panel-content">
                        <div class="log-container" id="logContainer">
                            <!-- Logs will be populated by JavaScript -->
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Global variables
        let progressChart = null;
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {
            initializeCharts();
            updateDashboard();
            
            // Auto-refresh every 5 seconds
            setInterval(updateDashboard, 5000);
        });
        
        function initializeCharts() {
            const ctx = document.getElementById('progressChart').getContext('2d');
            progressChart = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: ['Completed', 'Processing', 'Failed', 'Pending'],
                    datasets: [{
                        data: [0, 0, 0, 0],
                        backgroundColor: ['#10B981', '#F59E0B', '#EF4444', '#6B7280'],
                        borderWidth: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                padding: 20,
                                usePointStyle: true
                            }
                        }
                    }
                }
            });
        }
        
        async function updateDashboard() {
            try {
                // Update stats
                const statsResponse = await fetch('/api/stats');
                const stats = await statsResponse.json();
                updateStatsGrid(stats);
                
                // Update progress
                updateProgressBar(stats);
                updateProgressChart(stats);
                
                // Update tracks
                const tracksResponse = await fetch('/api/tracks?limit=10');
                const tracks = await tracksResponse.json();
                updateRecentTracks(tracks);
                
                // Update system resources
                const systemResponse = await fetch('/api/system');
                const system = await systemResponse.json();
                updateSystemResources(system);
                
                // Update logs
                const logsResponse = await fetch('/api/logs?lines=20');
                const logs = await logsResponse.json();
                updateLogs(logs);
                
            } catch (error) {
                console.error('Error updating dashboard:', error);
            }
        }
        
        function updateStatsGrid(stats) {
            const grid = document.getElementById('statsGrid');
            grid.innerHTML = `
                <div class="stat-card">
                    <h3>Total Tracks</h3>
                    <div class="value">${stats.total_tracks || 0}</div>
                    <div class="label">Found in channel</div>
                </div>
                <div class="stat-card">
                    <h3>Completed</h3>
                    <div class="value">${stats.completed_tracks || 0}</div>
                    <div class="label">${(stats.completion_percentage || 0).toFixed(1)}% processed</div>
                </div>
                <div class="stat-card">
                    <h3>Downloads</h3>
                    <div class="value">${stats.successful_downloads || 0}</div>
                    <div class="label">Successfully downloaded</div>
                </div>
                <div class="stat-card">
                    <h3>Uploads</h3>
                    <div class="value">${stats.successful_uploads || 0}</div>
                    <div class="label">Uploaded to cloud</div>
                </div>
                <div class="stat-card">
                    <h3>Runtime</h3>
                    <div class="value">${stats.runtime_formatted || '00:00:00'}</div>
                    <div class="label">Processing time</div>
                </div>
                <div class="stat-card">
                    <h3>Failed</h3>
                    <div class="value">${stats.failed_tracks || 0}</div>
                    <div class="label">Error cases</div>
                </div>
            `;
        }
        
        function updateProgressBar(stats) {
            const progressBar = document.getElementById('progressBar');
            const percentage = stats.completion_percentage || 0;
            progressBar.style.width = percentage + '%';
        }
        
        function updateProgressChart(stats) {
            if (progressChart) {
                progressChart.data.datasets[0].data = [
                    stats.completed_tracks || 0,
                    (stats.total_tracks || 0) - (stats.completed_tracks || 0) - (stats.failed_tracks || 0),
                    stats.failed_tracks || 0,
                    0  // Pending (calculated on backend if needed)
                ];
                progressChart.update();
            }
        }
        
        function updateRecentTracks(tracks) {
            const tbody = document.getElementById('recentTracks');
            if (!tracks.tracks || tracks.tracks.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4">No tracks available</td></tr>';
                return;
            }
            
            tbody.innerHTML = tracks.tracks.map(track => {
                const statusClass = `status-${track.processing_status}`;
                const timeAgo = getTimeAgo(track.updated_at);
                
                return `
                    <tr>
                        <td title="${track.title}">${truncate(track.title, 30)}</td>
                        <td title="${track.artist}">${truncate(track.artist, 20)}</td>
                        <td><span class="status-badge ${statusClass}">${track.processing_status}</span></td>
                        <td>${timeAgo}</td>
                    </tr>
                `;
            }).join('');
        }
        
        function updateSystemResources(system) {
            const container = document.getElementById('systemResources');
            container.innerHTML = `
                <div class="system-metric">
                    <span class="metric-label">CPU Usage</span>
                    <span class="metric-value">${system.cpu_percent || 0}%</span>
                </div>
                <div class="system-metric">
                    <span class="metric-label">Memory Usage</span>
                    <span class="metric-value">${system.memory_percent || 0}%</span>
                </div>
                <div class="system-metric">
                    <span class="metric-label">Disk Usage</span>
                    <span class="metric-value">${system.disk_percent || 0}%</span>
                </div>
                <div class="system-metric">
                    <span class="metric-label">Network I/O</span>
                    <span class="metric-value">${formatBytes(system.network_bytes || 0)}</span>
                </div>
                <div class="system-metric">
                    <span class="metric-label">Disk I/O</span>
                    <span class="metric-value">${formatBytes(system.disk_bytes || 0)}</span>
                </div>
            `;
        }
        
        function updateLogs(logs) {
            const container = document.getElementById('logContainer');
            if (logs.content) {
                container.innerHTML = logs.content.replace(/\\n/g, '<br>');
                container.scrollTop = container.scrollHeight;
            }
        }
        
        async function controlAction(action) {
            try {
                const response = await fetch(`/api/control/${action}`);
                const result = await response.json();
                
                if (result.success) {
                    updateControlStatus(action);
                } else {
                    alert(`Failed to ${action}: ${result.message}`);
                }
            } catch (error) {
                console.error(`Error executing ${action}:`, error);
                alert(`Error executing ${action}`);
            }
        }
        
        function updateControlStatus(action) {
            const status = document.getElementById('controlStatus');
            const indicator = status.querySelector('.status-indicator');
            
            switch(action) {
                case 'start':
                    indicator.className = 'status-indicator status-running';
                    status.innerHTML = '<span class="status-indicator status-running"></span>Automation Running';
                    break;
                case 'pause':
                    indicator.className = 'status-indicator status-warning';
                    status.innerHTML = '<span class="status-indicator status-warning"></span>Automation Paused';
                    break;
                case 'stop':
                    indicator.className = 'status-indicator status-stopped';
                    status.innerHTML = '<span class="status-indicator status-stopped"></span>Automation Stopped';
                    break;
            }
        }
        
        function truncate(str, length) {
            if (!str) return '';
            return str.length > length ? str.substring(0, length) + '...' : str;
        }
        
        function formatBytes(bytes) {
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
        }
        
        function getTimeAgo(timestamp) {
            if (!timestamp) return 'Unknown';
            
            const now = new Date();
            const past = new Date(timestamp);
            const diffMs = now - past;
            
            if (diffMs < 60000) return 'Just now';
            if (diffMs < 3600000) return Math.floor(diffMs / 60000) + 'm ago';
            if (diffMs < 86400000) return Math.floor(diffMs / 3600000) + 'h ago';
            return Math.floor(diffMs / 86400000) + 'd ago';
        }
    </script>
</body>
</html>'''
        return html_template
        
    def get_processing_stats(self) -> Dict:
        """Get current processing statistics from database"""
        try:
            if not os.path.exists(self.db_path):
                return self.get_default_stats()
                
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get total tracks
            cursor.execute("SELECT COUNT(*) FROM music_tracks")
            total_tracks = cursor.fetchone()[0]
            
            # Get tracks by status
            cursor.execute("SELECT processing_status, COUNT(*) FROM music_tracks GROUP BY processing_status")
            status_counts = dict(cursor.fetchall())
            
            # Calculate stats
            completed_tracks = status_counts.get('completed', 0) + status_counts.get('uploaded', 0)
            failed_tracks = status_counts.get('failed', 0)
            processing_tracks = status_counts.get('processing', 0) + status_counts.get('downloaded', 0)
            
            # Get additional metrics
            cursor.execute("SELECT COUNT(*) FROM music_tracks WHERE processing_status IN ('downloaded', 'organized')")
            successful_downloads = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM music_tracks WHERE gdrive_url IS NOT NULL")
            successful_uploads = cursor.fetchone()[0]
            
            conn.close()
            
            completion_percentage = (completed_tracks / total_tracks * 100) if total_tracks > 0 else 0
            
            return {
                'total_tracks': total_tracks,
                'completed_tracks': completed_tracks,
                'failed_tracks': failed_tracks,
                'processing_tracks': processing_tracks,
                'successful_downloads': successful_downloads,
                'successful_uploads': successful_uploads,
                'completion_percentage': completion_percentage,
                'runtime_formatted': self.get_uptime(),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting processing stats: {e}")
            return self.get_default_stats()
            
    def get_default_stats(self) -> Dict:
        """Get default stats when database is not available"""
        return {
            'total_tracks': 0,
            'completed_tracks': 0,
            'failed_tracks': 0,
            'processing_tracks': 0,
            'successful_downloads': 0,
            'successful_uploads': 0,
            'completion_percentage': 0,
            'runtime_formatted': self.get_uptime(),
            'timestamp': datetime.now().isoformat()
        }
        
    def get_tracks(self, page: int = 1, limit: int = 50, status: str = 'all') -> Dict:
        """Get tracks with pagination and filtering"""
        try:
            if not os.path.exists(self.db_path):
                return {'tracks': [], 'total': 0, 'page': page}
                
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            cursor = conn.cursor()
            
            # Build query based on status filter
            base_query = "FROM music_tracks"
            where_clause = ""
            params = []
            
            if status != 'all':
                where_clause = " WHERE processing_status = ?"
                params.append(status)
                
            # Get total count
            count_query = f"SELECT COUNT(*) {base_query} {where_clause}"
            cursor.execute(count_query, params)
            total_tracks = cursor.fetchone()[0]
            
            # Get tracks with pagination
            offset = (page - 1) * limit
            tracks_query = f"""
                SELECT file_id, title, artist, album, year, side, processing_status, 
                       updated_at, gdrive_url, duration, file_size
                {base_query} {where_clause}
                ORDER BY updated_at DESC
                LIMIT ? OFFSET ?
            """
            params.extend([limit, offset])
            
            cursor.execute(tracks_query, params)
            tracks = [dict(row) for row in cursor.fetchall()]
            
            conn.close()
            
            return {
                'tracks': tracks,
                'total': total_tracks,
                'page': page,
                'limit': limit,
                'total_pages': (total_tracks + limit - 1) // limit
            }
            
        except Exception as e:
            self.logger.error(f"Error getting tracks: {e}")
            return {'tracks': [], 'total': 0, 'page': page}
            
    def update_system_stats(self):
        """Update system resource statistics"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            
            # Network I/O
            network = psutil.net_io_counters()
            network_bytes = network.bytes_sent + network.bytes_recv
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            disk_bytes = disk_io.read_bytes + disk_io.write_bytes if disk_io else 0
            
            self.system_stats = {
                'cpu_percent': round(cpu_percent, 1),
                'memory_percent': round(memory_percent, 1),
                'disk_percent': round(disk_percent, 1),
                'network_bytes': network_bytes,
                'disk_bytes': disk_bytes,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error updating system stats: {e}")
            
    def get_log_content(self, log_type: str = 'main', lines: int = 100) -> Dict:
        """Get log file content"""
        try:
            log_files = {
                'main': 'logs/system/main.log',
                'scraping': 'logs/scraping/scraping.log',
                'upload': 'logs/upload/upload.log',
                'database': 'logs/database/database.log',
                'monitoring': 'logs/monitoring/dashboard.log'
            }
            
            log_file = log_files.get(log_type, log_files['main'])
            
            if not os.path.exists(log_file):
                return {
                    'content': f'Log file {log_file} not found',
                    'lines': 0,
                    'type': log_type
                }
                
            # Read last N lines
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                all_lines = f.readlines()
                last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
                
            content = ''.join(last_lines)
            
            return {
                'content': content,
                'lines': len(last_lines),
                'type': log_type,
                'total_lines': len(all_lines)
            }
            
        except Exception as e:
            self.logger.error(f"Error reading log file: {e}")
            return {
                'content': f'Error reading log: {e}',
                'lines': 0,
                'type': log_type
            }
            
    def handle_control_action(self, action: str) -> Dict:
        """Handle control actions (start, stop, pause)"""
        try:
            service_name = 'telegram-music-automation'
            
            if action == 'start':
                result = subprocess.run(['sudo', 'systemctl', 'start', service_name], 
                                      capture_output=True, text=True)
            elif action == 'stop':
                result = subprocess.run(['sudo', 'systemctl', 'stop', service_name], 
                                      capture_output=True, text=True)
            elif action == 'pause':
                # For pause, we'll stop the service (resume with start)
                result = subprocess.run(['sudo', 'systemctl', 'stop', service_name], 
                                      capture_output=True, text=True)
            else:
                return {'success': False, 'message': f'Unknown action: {action}'}
                
            success = result.returncode == 0
            message = result.stdout if success else result.stderr
            
            self.logger.info(f"Control action {action}: {'Success' if success else 'Failed'}")
            
            return {
                'success': success,
                'message': message,
                'action': action,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error handling control action {action}: {e}")
            return {
                'success': False,
                'message': str(e),
                'action': action
            }
            
    def get_uptime(self) -> str:
        """Get system uptime"""
        try:
            with open('/proc/uptime', 'r') as f:
                uptime_seconds = float(f.readline().split()[0])
                
            uptime_timedelta = timedelta(seconds=int(uptime_seconds))
            return str(uptime_timedelta).split('.')[0]  # Remove microseconds
            
        except:
            return "Unknown"
            
    def background_monitoring(self):
        """Background thread for continuous monitoring"""
        while True:
            try:
                self.update_system_stats()
                time.sleep(30)  # Update every 30 seconds
            except Exception as e:
                self.logger.error(f"Error in background monitoring: {e}")
                time.sleep(60)  # Wait longer on error
                
    def run(self, host='0.0.0.0', port=8080, debug=False):
        """Run the monitoring dashboard"""
        self.logger.info(f"Starting monitoring dashboard on {host}:{port}")
        
        # Create static directory for assets
        os.makedirs('static', exist_ok=True)
        
        try:
            self.app.run(host=host, port=port, debug=debug, threaded=True)
        except Exception as e:
            self.logger.error(f"Failed to start dashboard: {e}")
            raise

def main():
    """Main entry point for monitoring dashboard"""
    try:
        # Create required directories
        os.makedirs('logs/monitoring', exist_ok=True)
        os.makedirs('data', exist_ok=True)
        
        # Initialize and run dashboard
        monitor = AutomationMonitor()
        
        # Get port from environment variable or use default
        port = int(os.getenv('MONITORING_PORT', 8080))
        
        monitor.run(port=port)
        
    except KeyboardInterrupt:
        print("\n⚠️  Monitoring dashboard stopped by user")
    except Exception as e:
        print(f"❌ Failed to start monitoring dashboard: {e}")
        exit(1)

if __name__ == "__main__":
    main()