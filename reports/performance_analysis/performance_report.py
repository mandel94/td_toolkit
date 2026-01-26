#!/usr/bin/env python3
"""
Taxi Drivers Performance Analysis Report Generator

This script generates comprehensive website performance reports including:
- Historical data analysis
- JSON data export
- Professional PDF reports
- Comparative analysis between periods

Usage:
    python performance_report.py --start-date 2025-11-01 --end-date 2025-11-07
    python performance_report.py --start-date 2025-11-01 --end-date 2025-11-07 --max-periods 10
    python performance_report.py --start-date 2025-11-01 --end-date 2025-11-07 --output-dir ./custom_output
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Any, Optional
import json

# Add parent directories to path for imports
current_dir = Path(__file__).parent
sys.path.append(str(current_dir.parent.parent))

from ga4_api.ga4_api import Ga4Client
from metrics_extractor import PerformanceMetricsExtractor
from periods_manager import HistoricalPeriodsManager
from json_exporter import PerformanceDataExporter
from pdf_generator import PDFReportGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('performance_analysis.log')
    ]
)
logger = logging.getLogger(__name__)


class PerformanceReportOrchestrator:
    """
    Main orchestrator for the performance analysis and reporting process.
    
    Coordinates all components to generate comprehensive performance reports.
    """
    
    def __init__(
        self, 
        output_dir: str = "output",
        property_id: str = "394327334",
        min_date: str = "2025-01-01"
    ):
        """
        Initialize the report orchestrator.
        
        Args:
            output_dir: Directory for output files
            property_id: GA4 property ID
            min_date: Minimum date for historical analysis
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        try:
            self.ga4_client = Ga4Client()
            self.metrics_extractor = PerformanceMetricsExtractor(self.ga4_client, property_id)
            self.periods_manager = HistoricalPeriodsManager(min_date)
            self.json_exporter = PerformanceDataExporter(str(self.output_dir))
            self.pdf_generator = PDFReportGenerator(str(self.output_dir))
            
            logger.info("Successfully initialized PerformanceReportOrchestrator")
            
        except Exception as e:
            logger.error(f"Error initializing components: {str(e)}")
            raise
    
    def generate_complete_report(
        self,
        start_date: str,
        end_date: str,
        max_periods: Optional[int] = None,
        include_pdf: bool = True,
        include_json: bool = True,
        report_title: str = "Taxi Drivers Website Performance Report"
    ) -> Dict[str, str]:
        """
        Generate complete performance report with JSON and PDF outputs.
        
        Args:
            start_date: Analysis period start date (YYYY-MM-DD)
            end_date: Analysis period end date (YYYY-MM-DD)
            max_periods: Maximum number of historical periods to analyze
            include_pdf: Whether to generate PDF report
            include_json: Whether to generate JSON export
            report_title: Title for the report
            
        Returns:
            Dictionary with paths to generated files
        """
        logger.info(f"Generating complete performance report for {start_date} to {end_date}")
        
        try:
            # Step 1: Calculate all analysis periods
            logger.info("Calculating analysis periods...")
            all_periods = self.periods_manager.get_all_analysis_periods(
                start_date, end_date, max_periods
            )
            logger.info(f"Found {len(all_periods)} periods for analysis")
            
            # Step 2: Extract metrics for all periods
            logger.info("Extracting metrics for all periods...")
            periods_data = []
            
            for i, period_info in enumerate(all_periods):
                logger.info(f"Processing period {i+1}/{len(all_periods)}: {period_info['start_date']} to {period_info['end_date']}")
                
                try:
                    period_metrics = self.metrics_extractor.extract_period_metrics(
                        period_info['start_date'],
                        period_info['end_date'],
                        include_dimensions=True
                    )
                    
                    # Add period metadata
                    period_metrics['period_metadata'] = period_info
                    periods_data.append(period_metrics)
                    
                except Exception as e:
                    logger.warning(f"Failed to extract metrics for period {period_info['start_date']}: {str(e)}")
                    continue
            
            if not periods_data:
                raise ValueError("No valid period data was extracted")
            
            logger.info(f"Successfully extracted metrics for {len(periods_data)} periods")
            
            # Step 3: Prepare analysis metadata
            analysis_metadata = {
                'analysis_configuration': {
                    'target_start_date': start_date,
                    'target_end_date': end_date,
                    'max_periods_requested': max_periods,
                    'min_date_boundary': self.periods_manager.min_date.strftime('%Y-%m-%d'),
                    'total_periods_analyzed': len(periods_data)
                },
                'generation_info': {
                    'generated_at': datetime.now().isoformat(),
                    'tool_version': '1.0.0',
                    'ga4_property_id': self.metrics_extractor.property_id
                }
            }
            
            # Step 4: Generate outputs
            output_files = {}
            
            # Generate JSON export
            if include_json:
                logger.info("Generating JSON export...")
                json_filename = f"performance_analysis_{start_date.replace('-', '')}_{end_date.replace('-', '')}.json"
                json_path = self.json_exporter.export_complete_analysis(
                    periods_data, analysis_metadata, json_filename
                )
                output_files['json'] = json_path
                logger.info(f"JSON export completed: {json_path}")
            
            # Generate PDF report
            if include_pdf and len(periods_data) >= 2:
                logger.info("Generating PDF report...")
                
                # Get current and comparison periods
                current_period = periods_data[0]  # Most recent
                comparison_period = periods_data[1]  # Second most recent
                
                pdf_filename = f"performance_report_{start_date.replace('-', '')}_{end_date.replace('-', '')}.pdf"
                pdf_path = self.pdf_generator.generate_performance_report(
                    current_period,
                    comparison_period, 
                    periods_data,
                    report_title,
                    pdf_filename
                )
                output_files['pdf'] = pdf_path
                logger.info(f"PDF report completed: {pdf_path}")
            
            elif include_pdf:
                logger.warning("Insufficient data for PDF report generation (need at least 2 periods)")
            
            # Step 5: Generate summary report
            summary_path = self._generate_summary_report(periods_data, output_files)
            output_files['summary'] = summary_path
            
            logger.info("Performance report generation completed successfully")
            return output_files
            
        except Exception as e:
            logger.error(f"Error generating complete report: {str(e)}")
            raise
    
    def _generate_summary_report(
        self, 
        periods_data: List[Dict[str, Any]], 
        output_files: Dict[str, str]
    ) -> str:
        """Generate a text summary report."""
        
        try:
            summary_filename = f"performance_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            summary_path = self.output_dir / summary_filename
            
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write("TAXI DRIVERS WEBSITE PERFORMANCE ANALYSIS SUMMARY\n")
                f.write("=" * 55 + "\n\n")
                
                # Analysis overview
                f.write(f"Analysis completed: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}\n")
                f.write(f"Total periods analyzed: {len(periods_data)}\n\n")
                
                if periods_data:
                    current = periods_data[0]
                    current_period_info = current.get('period', {})
                    current_metrics = current.get('main_metrics', {})
                    
                    f.write("CURRENT PERIOD PERFORMANCE\n")
                    f.write("-" * 30 + "\n")
                    f.write(f"Period: {current_period_info.get('start_date')} to {current_period_info.get('end_date')}\n")
                    f.write(f"Page Views: {current_metrics.get('screenPageViews', 0):,.0f}\n")
                    f.write(f"Active Users: {current_metrics.get('activeUsers', 0):,.0f}\n")
                    f.write(f"New Users: {current_metrics.get('newUsers', 0):,.0f}\n")
                    f.write(f"Sessions: {current_metrics.get('sessions', 0):,.0f}\n")
                    f.write(f"Bounce Rate: {current_metrics.get('bounceRate', 0):.1%}\n")
                    f.write(f"Engagement Rate: {current_metrics.get('engagementRate', 0):.1%}\n\n")
                
                # Performance comparison
                if len(periods_data) >= 2:
                    comparison = periods_data[1]
                    comp_metrics = comparison.get('main_metrics', {})
                    
                    f.write("PERIOD-OVER-PERIOD CHANGES\n")
                    f.write("-" * 30 + "\n")
                    
                    metrics_to_compare = [
                        ('screenPageViews', 'Page Views'),
                        ('activeUsers', 'Active Users'),
                        ('newUsers', 'New Users'),
                        ('sessions', 'Sessions')
                    ]
                    
                    for metric_key, display_name in metrics_to_compare:
                        current_val = current_metrics.get(metric_key, 0)
                        previous_val = comp_metrics.get(metric_key, 0)
                        
                        if previous_val > 0:
                            change_pct = ((current_val - previous_val) / previous_val) * 100
                            change_abs = current_val - previous_val
                            f.write(f"{display_name}: {change_pct:+.1f}% ({change_abs:+,.0f})\n")
                
                f.write("\nGENERATED FILES\n")
                f.write("-" * 15 + "\n")
                for file_type, file_path in output_files.items():
                    f.write(f"{file_type.upper()}: {file_path}\n")
            
            return str(summary_path)
            
        except Exception as e:
            logger.error(f"Error generating summary report: {str(e)}")
            return ""
    
    def validate_inputs(self, start_date: str, end_date: str) -> bool:
        """
        Validate input parameters.
        
        Args:
            start_date: Start date string
            end_date: End date string
            
        Returns:
            True if inputs are valid
        """
        try:
            # Parse dates
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            # Validation checks
            if start_dt > end_dt:
                logger.error("Start date must be before end date")
                return False
            
            if start_dt < date(2025, 1, 1):
                logger.error("Start date cannot be before January 1, 2025")
                return False
            
            if end_dt > date.today():
                logger.error("End date cannot be in the future")
                return False
            
            # Check period length
            period_days = (end_dt - start_dt).days + 1
            if period_days > 365:
                logger.warning("Analysis period is longer than 1 year - this may take a while")
            
            return True
            
        except ValueError as e:
            logger.error(f"Invalid date format: {str(e)}")
            return False


def main():
    """Main entry point for the performance report generator."""
    
    parser = argparse.ArgumentParser(
        description='Generate comprehensive website performance reports for Taxi Drivers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Generate weekly report:
    python performance_report.py --start-date 2025-11-01 --end-date 2025-11-07
  
  Generate monthly report with limited history:
    python performance_report.py --start-date 2025-11-01 --end-date 2025-11-30 --max-periods 12
  
  Generate report to custom directory:
    python performance_report.py --start-date 2025-11-01 --end-date 2025-11-07 --output-dir ./reports
  
  Generate only JSON output:
    python performance_report.py --start-date 2025-11-01 --end-date 2025-11-07 --no-pdf
        """
    )
    
    # Required arguments
    parser.add_argument(
        '--start-date', 
        required=True,
        help='Start date for analysis period (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--end-date', 
        required=True,
        help='End date for analysis period (YYYY-MM-DD format)'
    )
    
    # Optional arguments
    parser.add_argument(
        '--output-dir',
        default='output',
        help='Output directory for generated files (default: output)'
    )
    
    parser.add_argument(
        '--max-periods',
        type=int,
        help='Maximum number of historical periods to analyze (default: all available)'
    )
    
    parser.add_argument(
        '--property-id',
        default='394327334',
        help='GA4 property ID (default: 394327334)'
    )
    
    parser.add_argument(
        '--min-date',
        default='2025-01-01',
        help='Minimum date for historical analysis (default: 2025-01-01)'
    )
    
    parser.add_argument(
        '--title',
        default='Taxi Drivers Website Performance Report',
        help='Title for the generated report'
    )
    
    parser.add_argument(
        '--no-pdf',
        action='store_true',
        help='Skip PDF report generation'
    )
    
    parser.add_argument(
        '--no-json',
        action='store_true',
        help='Skip JSON data export'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress all non-error output'
    )
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.quiet:
        logging.getLogger().setLevel(logging.ERROR)
    elif args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize orchestrator
        logger.info("Initializing Performance Report Orchestrator...")
        orchestrator = PerformanceReportOrchestrator(
            output_dir=args.output_dir,
            property_id=args.property_id,
            min_date=args.min_date
        )
        
        # Validate inputs
        if not orchestrator.validate_inputs(args.start_date, args.end_date):
            logger.error("Input validation failed")
            sys.exit(1)
        
        # Generate report
        logger.info("Starting report generation...")
        output_files = orchestrator.generate_complete_report(
            start_date=args.start_date,
            end_date=args.end_date,
            max_periods=args.max_periods,
            include_pdf=not args.no_pdf,
            include_json=not args.no_json,
            report_title=args.title
        )
        
        # Display results
        print("\n" + "="*60)
        print("PERFORMANCE REPORT GENERATION COMPLETED")
        print("="*60)
        print(f"Analysis Period: {args.start_date} to {args.end_date}")
        print(f"Output Directory: {args.output_dir}")
        print("\nGenerated Files:")
        for file_type, file_path in output_files.items():
            print(f"  {file_type.upper()}: {file_path}")
        
        print(f"\nReport generation completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Report generation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Report generation failed: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()