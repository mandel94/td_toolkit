"""
Test script for Performance Analysis System

This script provides basic functionality tests for the performance analysis system.
Run this to validate that all components are working correctly.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
import json
import tempfile
import shutil

# Add parent directories to path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent))

from metrics_extractor import PerformanceMetricsExtractor
from periods_manager import HistoricalPeriodsManager
from json_exporter import PerformanceDataExporter
from pdf_generator import PDFReportGenerator


class TestPerformanceAnalysisSystem(unittest.TestCase):
    """Test cases for the performance analysis system."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.start_date = "2025-11-01"
        self.end_date = "2025-11-07"
        
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_periods_manager_initialization(self):
        """Test HistoricalPeriodsManager initialization."""
        manager = HistoricalPeriodsManager()
        self.assertIsNotNone(manager)
        self.assertIsNotNone(manager.min_date)
    
    def test_periods_calculation(self):
        """Test historical periods calculation."""
        manager = HistoricalPeriodsManager()
        
        periods = manager.calculate_historical_periods(
            self.start_date, self.end_date, max_periods=5
        )
        
        self.assertIsInstance(periods, list)
        self.assertLessEqual(len(periods), 5)
        
        if periods:
            # Check period structure
            period = periods[0]
            required_fields = ['start_date', 'end_date', 'duration_days', 'period_label']
            for field in required_fields:
                self.assertIn(field, period)
    
    def test_json_exporter_initialization(self):
        """Test PerformanceDataExporter initialization."""
        exporter = PerformanceDataExporter(self.temp_dir)
        self.assertIsNotNone(exporter)
        self.assertTrue(os.path.exists(self.temp_dir))
    
    def test_json_export_structure(self):
        """Test JSON export data structure."""
        exporter = PerformanceDataExporter(self.temp_dir)
        
        # Create mock data
        mock_periods_data = [
            {
                'period': {
                    'start_date': '2025-11-01',
                    'end_date': '2025-11-07',
                    'duration_days': 7
                },
                'main_metrics': {
                    'screenPageViews': 1000,
                    'activeUsers': 500,
                    'sessions': 800
                },
                'data_quality': {
                    'completeness_score': 0.9,
                    'overall_quality': 'good'
                }
            }
        ]
        
        mock_metadata = {
            'analysis_configuration': {
                'target_start_date': '2025-11-01',
                'target_end_date': '2025-11-07'
            }
        }
        
        # Test export
        json_path = exporter.export_complete_analysis(
            mock_periods_data, mock_metadata, "test_export.json"
        )
        
        self.assertTrue(os.path.exists(json_path))
        
        # Validate JSON structure
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        required_fields = ['schema_version', 'export_metadata', 'periods_data']
        for field in required_fields:
            self.assertIn(field, data)
    
    def test_pdf_generator_initialization(self):
        """Test PDFReportGenerator initialization."""
        generator = PDFReportGenerator(self.temp_dir)
        self.assertIsNotNone(generator)
        self.assertIsNotNone(generator.colors)
        self.assertIsNotNone(generator.styles)
    
    def test_date_validation(self):
        """Test date validation logic."""
        manager = HistoricalPeriodsManager()
        
        # Test valid dates
        periods = manager.get_all_analysis_periods(
            "2025-11-01", "2025-11-07", max_periods=3
        )
        self.assertIsInstance(periods, list)
        self.assertGreater(len(periods), 0)
        
        # Test target period is first
        if periods:
            target_period = periods[0]
            self.assertTrue(target_period.get('is_target_period', False))
    
    def test_period_validation(self):
        """Test period validation functionality."""
        manager = HistoricalPeriodsManager()
        
        # Create test periods
        test_periods = [
            {
                'start_date': '2025-11-01',
                'end_date': '2025-11-07', 
                'duration_days': 7
            },
            {
                'start_date': '2025-10-25',
                'end_date': '2025-10-31',
                'duration_days': 7
            }
        ]
        
        validation = manager.validate_period_alignment(test_periods)
        
        # Should have validation results
        self.assertIn('consistent_duration', validation)
        self.assertIn('valid_date_ranges', validation)
        self.assertIn('no_overlaps', validation)


class TestSystemIntegration(unittest.TestCase):
    """Integration tests for the complete system."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up integration test fixtures."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_end_to_end_mock_flow(self):
        """Test end-to-end flow with mock data."""
        # This test would require GA4 API access, so we'll test component integration instead
        
        # Test periods manager
        periods_manager = HistoricalPeriodsManager()
        periods = periods_manager.get_all_analysis_periods(
            "2025-11-01", "2025-11-07", max_periods=3
        )
        
        self.assertGreater(len(periods), 0)
        
        # Test JSON exporter with periods data
        json_exporter = PerformanceDataExporter(self.temp_dir)
        
        # Create mock metrics data for each period
        mock_periods_data = []
        for period in periods:
            mock_data = {
                'period': period,
                'main_metrics': {
                    'screenPageViews': 1000,
                    'activeUsers': 500,
                    'newUsers': 200,
                    'sessions': 800,
                    'bounceRate': 0.65,
                    'engagementRate': 0.35
                },
                'derived_metrics': {
                    'pages_per_session': 1.25,
                    'new_user_rate': 0.4
                },
                'data_quality': {
                    'completeness_score': 0.9,
                    'overall_quality': 'good'
                }
            }
            mock_periods_data.append(mock_data)
        
        # Test JSON export
        mock_metadata = {'test': True}
        json_path = json_exporter.export_complete_analysis(
            mock_periods_data, mock_metadata, "integration_test.json"
        )
        
        self.assertTrue(os.path.exists(json_path))
        
        # Validate exported data can be loaded
        loaded_data = json_exporter.load_exported_data(json_path)
        self.assertIn('periods_data', loaded_data)
        self.assertEqual(len(loaded_data['periods_data']), len(mock_periods_data))


def run_system_test():
    """Run a simple system test."""
    print("🧪 Running Performance Analysis System Tests...")
    print("="*50)
    
    # Basic component tests
    try:
        print("Testing HistoricalPeriodsManager...")
        manager = HistoricalPeriodsManager()
        periods = manager.calculate_historical_periods(
            "2025-11-01", "2025-11-07", max_periods=3
        )
        print(f"✅ Generated {len(periods)} historical periods")
        
        print("\nTesting PerformanceDataExporter...")
        temp_dir = tempfile.mkdtemp()
        exporter = PerformanceDataExporter(temp_dir)
        
        # Mock data
        mock_data = [{
            'period': {'start_date': '2025-11-01', 'end_date': '2025-11-07'},
            'main_metrics': {'screenPageViews': 1000, 'activeUsers': 500}
        }]
        
        json_path = exporter.export_complete_analysis(
            mock_data, {'test': True}, "test.json"
        )
        print(f"✅ JSON export successful: {json_path}")
        
        # Cleanup
        shutil.rmtree(temp_dir)
        
        print("\n🎉 All basic tests passed!")
        print("\nTo run full tests: python -m unittest test_performance_analysis.py")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False
    
    return True


if __name__ == '__main__':
    # Run simple system test
    if len(sys.argv) > 1 and sys.argv[1] == 'simple':
        run_system_test()
    else:
        # Run full unit tests
        unittest.main()