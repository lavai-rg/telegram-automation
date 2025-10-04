#!/usr/bin/env python3
"""
Configuration and Utilities for Telegram History Scraping
Specialized configurations for different history scraping scenarios
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import os
import json

@dataclass
class HistoryScrapingProfiles:
    """Pre-defined profiles for different history scraping needs"""
    
    # Complete Archive Profile (Most comprehensive)
    COMPLETE_ARCHIVE = {
        'name': 'Complete Archive',
        'description': 'Scrape entire channel history from beginning',
        'max_messages': None,  # No limit
        'batch_size': 100,
        'delay_between_batches': 3.0,
        'start_date': None,  # From beginning
        'end_date': None,    # To present
        'save_raw_data': True,
        'save_metadata_json': True,
        'save_metadata_csv': True,
        'generate_statistics': True,
        'estimated_time': '2-6 hours depending on channel size'
    }
    
    # Recent History Profile (Last 1-2 years)
    RECENT_HISTORY = {
        'name': 'Recent History',
        'description': 'Scrape recent history (last 1-2 years)',
        'max_messages': 5000,
        'batch_size': 150,
        'delay_between_batches': 2.0,
        'start_date': datetime.now() - timedelta(days=730),  # 2 years ago
        'end_date': None,
        'save_raw_data': True,
        'save_metadata_json': True,
        'save_metadata_csv': True,
        'generate_statistics': True,
        'estimated_time': '30-60 minutes'
    }
    
    # Vintage Collection Profile (Older content)
    VINTAGE_COLLECTION = {
        'name': 'Vintage Collection',
        'description': 'Focus on older historical content',
        'max_messages': 3000,
        'batch_size': 75,
        'delay_between_batches': 2.5,
        'start_date': datetime(2015, 1, 1),
        'end_date': datetime(2020, 12, 31),
        'save_raw_data': True,
        'save_metadata_json': True,
        'save_metadata_csv': True,
        'generate_statistics': True,
        'estimated_time': '45-90 minutes'
    }
    
    # Metadata Only Profile (No file downloads)
    METADATA_ONLY = {
        'name': 'Metadata Only',
        'description': 'Extract only metadata, no file downloads',
        'max_messages': 10000,
        'batch_size': 200,
        'delay_between_batches': 1.5,
        'start_date': None,
        'end_date': None,
        'save_raw_data': False,
        'save_metadata_json': True,
        'save_metadata_csv': True,
        'generate_statistics': True,
        'download_files': False,
        'estimated_time': '15-30 minutes'
    }
    
    # Sample Profile (Quick testing)
    SAMPLE_PROFILE = {
        'name': 'Sample Profile',
        'description': 'Small sample for testing',
        'max_messages': 100,
        'batch_size': 25,
        'delay_between_batches': 1.0,
        'start_date': None,
        'end_date': None,
        'save_raw_data': True,
        'save_metadata_json': True,
        'save_metadata_csv': False,
        'generate_statistics': False,
        'estimated_time': '2-5 minutes'
    }

class HistoryScrapingManager:
    """Manager class for different history scraping operations"""
    
    def __init__(self):
        self.profiles = HistoryScrapingProfiles()
        self.current_session = None
    
    def get_profile_by_name(self, profile_name: str) -> Dict:
        """Get scraping profile by name"""
        profiles = {
            'complete': self.profiles.COMPLETE_ARCHIVE,
            'recent': self.profiles.RECENT_HISTORY,
            'vintage': self.profiles.VINTAGE_COLLECTION,
            'metadata': self.profiles.METADATA_ONLY,
            'sample': self.profiles.SAMPLE_PROFILE
        }
        
        return profiles.get(profile_name.lower(), self.profiles.COMPLETE_ARCHIVE)
    
    def create_custom_profile(self, 
                            name: str,
                            max_messages: Optional[int] = None,
                            start_date: Optional[datetime] = None,
                            end_date: Optional[datetime] = None,
                            batch_size: int = 100,
                            delay: float = 2.0) -> Dict:
        """Create custom scraping profile"""
        
        return {
            'name': name,
            'description': f'Custom profile: {name}',
            'max_messages': max_messages,
            'batch_size': batch_size,
            'delay_between_batches': delay,
            'start_date': start_date,
            'end_date': end_date,
            'save_raw_data': True,
            'save_metadata_json': True,
            'save_metadata_csv': True,
            'generate_statistics': True,
            'custom_profile': True
        }
    
    def estimate_scraping_time(self, profile: Dict, estimated_total_messages: int) -> str:
        """Estimate scraping time based on profile and message count"""
        
        batch_size = profile.get('batch_size', 100)
        delay = profile.get('delay_between_batches', 2.0)
        max_messages = profile.get('max_messages', estimated_total_messages)
        
        # Calculate effective messages to process
        messages_to_process = min(max_messages or estimated_total_messages, estimated_total_messages)
        
        # Calculate batches needed
        batches_needed = (messages_to_process + batch_size - 1) // batch_size
        
        # Calculate time
        processing_time_per_batch = 2  # seconds average
        total_delay_time = (batches_needed - 1) * delay
        total_processing_time = batches_needed * processing_time_per_batch
        
        total_seconds = total_delay_time + total_processing_time
        
        if total_seconds < 60:
            return f"{int(total_seconds)} seconds"
        elif total_seconds < 3600:
            return f"{int(total_seconds / 60)} minutes"
        else:
            hours = int(total_seconds / 3600)
            minutes = int((total_seconds % 3600) / 60)
            return f"{hours}h {minutes}m"

class HistoryDataAnalyzer:
    """Analyzer for historical data patterns"""
    
    def __init__(self, data_path: str):
        self.data_path = data_path
        self.data = None
        
    def load_data(self) -> List[Dict]:
        """Load scraped historical data"""
        try:
            with open(self.data_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Handle different data formats
            if isinstance(data, dict) and 'data' in data:
                self.data = data['data']
            elif isinstance(data, list):
                self.data = data
            else:
                self.data = []
                
            return self.data
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return []
    
    def analyze_temporal_patterns(self) -> Dict:
        """Analyze temporal patterns in the data"""
        if not self.data:
            return {}
        
        from collections import defaultdict
        
        patterns = {
            'monthly_distribution': defaultdict(int),
            'yearly_distribution': defaultdict(int),
            'daily_pattern': defaultdict(int),
            'hourly_pattern': defaultdict(int)
        }
        
        for record in self.data:
            try:
                date_obj = datetime.fromisoformat(record['date'].replace('Z', '+00:00'))
                
                # Monthly distribution
                month_key = date_obj.strftime('%Y-%m')
                patterns['monthly_distribution'][month_key] += 1
                
                # Yearly distribution
                year_key = str(date_obj.year)
                patterns['yearly_distribution'][year_key] += 1
                
                # Daily pattern (day of week)
                day_key = date_obj.strftime('%A')
                patterns['daily_pattern'][day_key] += 1
                
                # Hourly pattern
                hour_key = str(date_obj.hour)
                patterns['hourly_pattern'][hour_key] += 1
                
            except Exception as e:
                continue
        
        # Convert defaultdicts to regular dicts and sort
        for key in patterns:
            patterns[key] = dict(sorted(patterns[key].items()))
        
        return patterns
    
    def analyze_content_patterns(self) -> Dict:
        """Analyze content patterns"""
        if not self.data:
            return {}
        
        from collections import Counter
        
        patterns = {
            'top_artists': Counter(),
            'year_distribution': Counter(),
            'genre_keywords': Counter(),
            'file_formats': Counter(),
            'size_distribution': {'small': 0, 'medium': 0, 'large': 0, 'very_large': 0}
        }
        
        genre_keywords = [
            'rock', 'jazz', 'classical', 'pop', 'folk', 'blues', 'country', 
            'electronic', 'hip-hop', 'reggae', 'metal', 'punk', 'disco'
        ]
        
        for record in self.data:
            # Artist analysis
            if record.get('artist'):
                patterns['top_artists'][record['artist']] += 1
            
            # Year analysis
            if record.get('year'):
                patterns['year_distribution'][record['year']] += 1
            
            # Genre keyword analysis
            text = (record.get('raw_text', '') + ' ' + record.get('description', '')).lower()
            for genre in genre_keywords:
                if genre in text:
                    patterns['genre_keywords'][genre] += 1
            
            # File format analysis
            if record.get('mime_type'):
                patterns['file_formats'][record['mime_type']] += 1
            
            # Size distribution
            file_size = record.get('file_size', 0)
            if file_size < 5 * 1024 * 1024:  # < 5MB
                patterns['size_distribution']['small'] += 1
            elif file_size < 20 * 1024 * 1024:  # < 20MB
                patterns['size_distribution']['medium'] += 1
            elif file_size < 100 * 1024 * 1024:  # < 100MB
                patterns['size_distribution']['large'] += 1
            else:
                patterns['size_distribution']['very_large'] += 1
        
        # Convert Counters to regular dicts with top items
        patterns['top_artists'] = dict(patterns['top_artists'].most_common(20))
        patterns['year_distribution'] = dict(patterns['year_distribution'].most_common())
        patterns['genre_keywords'] = dict(patterns['genre_keywords'].most_common(10))
        patterns['file_formats'] = dict(patterns['file_formats'].most_common())
        
        return patterns
    
    def generate_analysis_report(self, output_path: str = None) -> Dict:
        """Generate comprehensive analysis report"""
        
        temporal = self.analyze_temporal_patterns()
        content = self.analyze_content_patterns()
        
        # Calculate summary statistics
        total_records = len(self.data) if self.data else 0
        
        total_size = sum(record.get('file_size', 0) for record in self.data) if self.data else 0
        total_duration = sum(record.get('duration', 0) for record in self.data) if self.data else 0
        
        unique_artists = len(set(record.get('artist', '') for record in self.data if record.get('artist'))) if self.data else 0
        
        date_range = {'earliest': None, 'latest': None}
        if self.data:
            dates = [record.get('date') for record in self.data if record.get('date')]
            if dates:
                date_range['earliest'] = min(dates)
                date_range['latest'] = max(dates)
        
        report = {
            'summary': {
                'total_records': total_records,
                'unique_artists': unique_artists,
                'total_size_gb': round(total_size / (1024**3), 2),
                'total_duration_hours': round(total_duration / 3600, 2),
                'date_range': date_range,
                'analysis_generated_at': datetime.now().isoformat()
            },
            'temporal_patterns': temporal,
            'content_patterns': content,
            'recommendations': self._generate_recommendations(temporal, content, total_records)
        }
        
        # Save report if output path provided
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"Analysis report saved to: {output_path}")
        
        return report
    
    def _generate_recommendations(self, temporal: Dict, content: Dict, total_records: int) -> List[str]:
        """Generate recommendations based on analysis"""
        recommendations = []
        
        # Temporal recommendations
        if temporal.get('monthly_distribution'):
            peak_months = sorted(temporal['monthly_distribution'].items(), 
                               key=lambda x: x[1], reverse=True)[:3]
            if peak_months:
                recommendations.append(
                    f"Peak activity months: {', '.join([month for month, count in peak_months])} "
                    f"- consider focusing scraping efforts during these periods"
                )
        
        # Content recommendations
        if content.get('top_artists'):
            top_artist_count = list(content['top_artists'].values())[0] if content['top_artists'] else 0
            if top_artist_count > total_records * 0.05:  # More than 5% from single artist
                recommendations.append(
                    f"High concentration from single artist detected - consider creating "
                    f"separate collection strategies"
                )
        
        # Size recommendations
        size_dist = content.get('size_distribution', {})
        large_files_ratio = (size_dist.get('large', 0) + size_dist.get('very_large', 0)) / max(total_records, 1)
        if large_files_ratio > 0.3:
            recommendations.append(
                "High proportion of large files detected - consider implementing "
                "compression or selective download strategies"
            )
        
        # Format recommendations
        formats = content.get('file_formats', {})
        if len(formats) > 5:
            recommendations.append(
                f"Multiple file formats detected ({len(formats)} types) - consider "
                f"format standardization in post-processing"
            )
        
        return recommendations

# Utility functions for quick setup
def create_quick_config(profile_name: str = 'recent', 
                       api_id: str = '', 
                       api_hash: str = '', 
                       phone: str = '') -> Dict:
    """Create quick configuration for history scraping"""
    
    manager = HistoryScrapingManager()
    profile = manager.get_profile_by_name(profile_name)
    
    config = {
        'api_credentials': {
            'api_id': api_id,
            'api_hash': api_hash,
            'phone_number': phone
        },
        'scraping_profile': profile,
        'output_settings': {
            'base_dir': './telegram_history_output',
            'create_subdirs': True,
            'timestamp_folders': True
        },
        'resumption': {
            'enable_checkpoints': True,
            'checkpoint_interval': 50
        }
    }
    
    return config

def print_available_profiles():
    """Print all available scraping profiles"""
    profiles = HistoryScrapingProfiles()
    
    print("🎵 Available History Scraping Profiles:")
    print("=" * 50)
    
    profile_list = [
        ('complete', profiles.COMPLETE_ARCHIVE),
        ('recent', profiles.RECENT_HISTORY),
        ('vintage', profiles.VINTAGE_COLLECTION),
        ('metadata', profiles.METADATA_ONLY),
        ('sample', profiles.SAMPLE_PROFILE)
    ]
    
    for name, profile in profile_list:
        print(f"\n📋 {profile['name']} ({name})")
        print(f"   Description: {profile['description']}")
        print(f"   Max Messages: {profile['max_messages'] or 'Unlimited'}")
        print(f"   Batch Size: {profile['batch_size']}")
        print(f"   Estimated Time: {profile['estimated_time']}")
        
        if profile.get('start_date'):
            print(f"   Date Range: {profile['start_date'].strftime('%Y-%m-%d')} to {profile.get('end_date', 'Present')}")

if __name__ == "__main__":
    # Demo usage
    print_available_profiles()
    
    print("\n" + "=" * 50)
    print("📊 Sample Analysis Demo")
    print("=" * 50)
    
    # Demo analysis (would require actual data file)
    # analyzer = HistoryDataAnalyzer('sample_data.json')
    # analyzer.load_data()
    # report = analyzer.generate_analysis_report()
    # print(json.dumps(report['summary'], indent=2))