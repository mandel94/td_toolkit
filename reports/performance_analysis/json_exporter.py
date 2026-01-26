"""
JSON Data Exporter for Performance Analysis

This module handles structured export of performance data to JSON format,
following data engineering best practices for data serialization and metadata management.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pathlib import Path
import hashlib
import os

logger = logging.getLogger(__name__)


class PerformanceDataExporter:
    """
    Exports performance analysis data to structured JSON format.
    
    Features:
    - Schema-compliant JSON structure
    - Comprehensive metadata inclusion
    - Data integrity validation
    - Configurable export options
    - Error handling and logging
    """
    
    def __init__(self, output_dir: str = "output"):
        """
        Initialize the data exporter.
        
        Args:
            output_dir: Directory for JSON output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # JSON schema version for compatibility tracking
        self.schema_version = "1.0.0"
        
        logger.info(f"Initialized PerformanceDataExporter with output_dir={output_dir}")
    
    def export_complete_analysis(
        self,
        periods_data: List[Dict[str, Any]],
        analysis_metadata: Dict[str, Any],
        filename: Optional[str] = None
    ) -> str:
        """
        Export complete performance analysis to JSON.
        
        Args:
            periods_data: List of period data from metrics extraction
            analysis_metadata: Metadata about the analysis configuration
            filename: Custom filename (auto-generated if None)
            
        Returns:
            Path to exported JSON file
        """
        logger.info("Exporting complete performance analysis to JSON")
        
        try:
            # Generate filename if not provided
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"taxi_drivers_performance_analysis_{timestamp}.json"
            
            # Ensure .json extension
            if not filename.endswith('.json'):
                filename += '.json'
            
            file_path = self.output_dir / filename
            
            # Build complete JSON structure
            export_data = self._build_complete_export_structure(
                periods_data, analysis_metadata
            )
            
            # Validate export data
            self._validate_export_data(export_data)
            
            # Write to JSON file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False, default=self._json_serializer)
            
            logger.info(f"Successfully exported analysis to {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error exporting complete analysis: {str(e)}")
            raise
    
    def export_periods_summary(
        self,
        periods_data: List[Dict[str, Any]],
        filename: Optional[str] = None
    ) -> str:
        """
        Export a summary of all periods data to JSON.
        
        Args:
            periods_data: List of period data from metrics extraction
            filename: Custom filename (auto-generated if None)
            
        Returns:
            Path to exported JSON file
        """
        logger.info("Exporting periods summary to JSON")
        
        try:
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"taxi_drivers_periods_summary_{timestamp}.json"
            
            if not filename.endswith('.json'):
                filename += '.json'
                
            file_path = self.output_dir / filename
            
            # Build summary structure
            summary_data = self._build_summary_structure(periods_data)
            
            # Write to JSON file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False, default=self._json_serializer)
            
            logger.info(f"Successfully exported periods summary to {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error exporting periods summary: {str(e)}")
            raise
    
    def _build_complete_export_structure(
        self,
        periods_data: List[Dict[str, Any]],
        analysis_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build the complete export data structure."""
        
        # Calculate summary statistics
        summary_stats = self._calculate_summary_statistics(periods_data)
        
        export_structure = {
            "schema_version": self.schema_version,
            "export_metadata": {
                "export_timestamp": datetime.now(timezone.utc).isoformat(),
                "export_tool": "Taxi Drivers Performance Analysis Suite",
                "total_periods": len(periods_data),
                "analysis_range": self._get_analysis_range(periods_data),
                "data_integrity_hash": self._calculate_data_hash(periods_data)
            },
            "analysis_configuration": analysis_metadata,
            "summary_statistics": summary_stats,
            "periods_data": periods_data,
            "data_quality_report": self._generate_data_quality_report(periods_data)
        }
        
        return export_structure
    
    def _build_summary_structure(self, periods_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build a summary-only export structure."""
        
        summary_structure = {
            "schema_version": self.schema_version,
            "export_metadata": {
                "export_timestamp": datetime.now(timezone.utc).isoformat(),
                "export_type": "summary",
                "total_periods": len(periods_data)
            },
            "periods_summary": []
        }
        
        # Extract summary for each period
        for period_data in periods_data:
            period_summary = {
                "period": period_data.get('period', {}),
                "key_metrics_summary": self._extract_key_metrics_summary(period_data),
                "data_quality": period_data.get('data_quality', {}),
                "extraction_timestamp": period_data.get('extraction_timestamp')
            }
            summary_structure["periods_summary"].append(period_summary)
        
        return summary_structure
    
    def _extract_key_metrics_summary(self, period_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract key metrics summary from period data."""
        main_metrics = period_data.get('main_metrics', {})
        derived_metrics = period_data.get('derived_metrics', {})
        
        key_summary = {
            # Core traffic metrics
            'screenPageViews': main_metrics.get('screenPageViews', 0),
            'activeUsers': main_metrics.get('activeUsers', 0),
            'newUsers': main_metrics.get('newUsers', 0),
            'sessions': main_metrics.get('sessions', 0),
            
            # Engagement metrics
            'bounceRate': main_metrics.get('bounceRate', 0),
            'engagementRate': main_metrics.get('engagementRate', 0),
            'averageSessionDuration': main_metrics.get('averageSessionDuration', 0),
            
            # Derived metrics
            'pages_per_session': derived_metrics.get('pages_per_session', 0),
            'new_user_rate': derived_metrics.get('new_user_rate', 0),
            'returning_user_rate': derived_metrics.get('returning_user_rate', 0)
        }
        
        return key_summary
    
    def _calculate_summary_statistics(self, periods_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate summary statistics across all periods."""
        if not periods_data:
            return {}
        
        # Collect key metrics across all periods
        all_page_views = [p.get('main_metrics', {}).get('screenPageViews', 0) for p in periods_data]
        all_users = [p.get('main_metrics', {}).get('activeUsers', 0) for p in periods_data]
        all_sessions = [p.get('main_metrics', {}).get('sessions', 0) for p in periods_data]
        
        summary_stats = {
            "page_views": {
                "total": sum(all_page_views),
                "average": sum(all_page_views) / len(all_page_views) if all_page_views else 0,
                "max": max(all_page_views) if all_page_views else 0,
                "min": min(all_page_views) if all_page_views else 0
            },
            "active_users": {
                "total": sum(all_users),
                "average": sum(all_users) / len(all_users) if all_users else 0,
                "max": max(all_users) if all_users else 0,
                "min": min(all_users) if all_users else 0
            },
            "sessions": {
                "total": sum(all_sessions),
                "average": sum(all_sessions) / len(all_sessions) if all_sessions else 0,
                "max": max(all_sessions) if all_sessions else 0,
                "min": min(all_sessions) if all_sessions else 0
            }
        }
        
        return summary_stats
    
    def _generate_data_quality_report(self, periods_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate overall data quality report."""
        if not periods_data:
            return {"status": "no_data"}
        
        # Analyze data quality across all periods
        quality_scores = [p.get('data_quality', {}).get('completeness_score', 0) for p in periods_data]
        overall_qualities = [p.get('data_quality', {}).get('overall_quality', 'unknown') for p in periods_data]
        
        quality_report = {
            "overall_completeness": {
                "average_score": sum(quality_scores) / len(quality_scores) if quality_scores else 0,
                "min_score": min(quality_scores) if quality_scores else 0,
                "max_score": max(quality_scores) if quality_scores else 0
            },
            "quality_distribution": {
                quality: overall_qualities.count(quality) for quality in set(overall_qualities)
            },
            "periods_with_issues": len([p for p in periods_data 
                                     if p.get('data_quality', {}).get('overall_quality') in ['poor', 'fair']]),
            "total_periods_analyzed": len(periods_data)
        }
        
        return quality_report
    
    def _get_analysis_range(self, periods_data: List[Dict[str, Any]]) -> Dict[str, str]:
        """Get the overall date range covered by the analysis."""
        if not periods_data:
            return {}
        
        all_start_dates = [p.get('period', {}).get('start_date') for p in periods_data if p.get('period', {}).get('start_date')]
        all_end_dates = [p.get('period', {}).get('end_date') for p in periods_data if p.get('period', {}).get('end_date')]
        
        if all_start_dates and all_end_dates:
            return {
                "earliest_start": min(all_start_dates),
                "latest_end": max(all_end_dates),
                "total_span_days": (datetime.strptime(max(all_end_dates), '%Y-%m-%d') - 
                                   datetime.strptime(min(all_start_dates), '%Y-%m-%d')).days + 1
            }
        
        return {}
    
    def _calculate_data_hash(self, periods_data: List[Dict[str, Any]]) -> str:
        """Calculate hash for data integrity verification."""
        try:
            # Create a deterministic string representation of the key data
            data_string = json.dumps(periods_data, sort_keys=True, default=str)
            return hashlib.sha256(data_string.encode()).hexdigest()[:16]  # First 16 chars
        except Exception as e:
            logger.warning(f"Could not calculate data hash: {str(e)}")
            return "hash_unavailable"
    
    def _validate_export_data(self, export_data: Dict[str, Any]) -> None:
        """Validate export data structure and content."""
        required_fields = [
            "schema_version", 
            "export_metadata", 
            "periods_data"
        ]
        
        for field in required_fields:
            if field not in export_data:
                raise ValueError(f"Missing required field in export data: {field}")
        
        if not isinstance(export_data["periods_data"], list):
            raise ValueError("periods_data must be a list")
        
        if len(export_data["periods_data"]) == 0:
            logger.warning("Exporting empty periods_data")
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for special data types."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return str(obj)
    
    def load_exported_data(self, filepath: str) -> Dict[str, Any]:
        """
        Load previously exported performance data from JSON.
        
        Args:
            filepath: Path to JSON file
            
        Returns:
            Loaded data dictionary
        """
        logger.info(f"Loading exported data from {filepath}")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Validate loaded data
            if 'schema_version' not in data:
                logger.warning("Loaded data missing schema_version - may be incompatible")
            
            logger.info(f"Successfully loaded data from {filepath}")
            return data
            
        except Exception as e:
            logger.error(f"Error loading exported data: {str(e)}")
            raise