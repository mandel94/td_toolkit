"""
PDF Report Generator for Performance Analysis

This module generates professional PDF reports with charts, tables, and comparative analysis
for Taxi Drivers website performance data.
"""

import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, cm
from reportlab.lib.colors import Color, HexColor
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.graphics.shapes import Drawing, Rect
from reportlab.graphics.charts.linecharts import HorizontalLineChart
from reportlab.graphics.charts.barcharts import VerticalBarChart
import pandas as pd
from pathlib import Path
import io
import base64

logger = logging.getLogger(__name__)

# Set matplotlib backend for server environments
plt.switch_backend('Agg')

# Configure style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10


class PDFReportGenerator:
    """
    Generates professional PDF reports for website performance analysis.
    
    Features:
    - Executive summary with key insights
    - Comparative analysis between periods
    - Visual charts and graphs
    - Professional layout and styling
    - Data tables with performance metrics
    """
    
    def __init__(self, output_dir: str = "output"):
        """
        Initialize the PDF report generator.
        
        Args:
            output_dir: Directory for PDF output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Corporate colors for Taxi Drivers branding
        self.colors = {
            'primary': HexColor('#1f4e79'),      # Dark blue
            'secondary': HexColor('#ff6b35'),     # Orange
            'accent': HexColor('#2e8b57'),        # Sea green
            'light_gray': HexColor('#f5f5f5'),
            'dark_gray': HexColor('#333333'),
            'success': HexColor('#28a745'),
            'warning': HexColor('#ffc107'),
            'danger': HexColor('#dc3545')
        }
        
        # Setup styles
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
        
        logger.info(f"Initialized PDFReportGenerator with output_dir={output_dir}")
    
    def generate_performance_report(
        self,
        current_period_data: Dict[str, Any],
        comparison_period_data: Dict[str, Any],
        all_periods_data: List[Dict[str, Any]],
        report_title: str = "Website Performance Report",
        filename: Optional[str] = None
    ) -> str:
        """
        Generate complete performance report PDF.
        
        Args:
            current_period_data: Data for the most recent period
            comparison_period_data: Data for comparison period  
            all_periods_data: All historical periods data
            report_title: Title for the report
            filename: Custom filename (auto-generated if None)
            
        Returns:
            Path to generated PDF file
        """
        logger.info("Generating performance report PDF")
        
        try:
            # Generate filename if not provided
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"taxi_drivers_performance_report_{timestamp}.pdf"
            
            if not filename.endswith('.pdf'):
                filename += '.pdf'
            
            file_path = self.output_dir / filename
            
            # Create PDF document
            doc = SimpleDocTemplate(
                str(file_path),
                pagesize=A4,
                topMargin=2*cm,
                bottomMargin=2*cm,
                leftMargin=2*cm,
                rightMargin=2*cm
            )
            
            # Build report content
            story = []
            
            # Title page
            story.extend(self._create_title_page(report_title, current_period_data, comparison_period_data))
            story.append(PageBreak())
            
            # Executive summary
            story.extend(self._create_executive_summary(current_period_data, comparison_period_data))
            story.append(PageBreak())
            
            # Key metrics comparison
            story.extend(self._create_metrics_comparison(current_period_data, comparison_period_data))
            story.append(PageBreak())
            
            # Performance trends
            story.extend(self._create_trends_analysis(all_periods_data))
            story.append(PageBreak())
            
            # Detailed breakdown
            story.extend(self._create_detailed_breakdown(current_period_data, comparison_period_data))
            
            # Generate charts and add them
            charts_paths = self._generate_charts(current_period_data, comparison_period_data, all_periods_data)
            if charts_paths:
                story.append(PageBreak())
                story.extend(self._add_charts_to_story(charts_paths))
            
            # Build PDF
            doc.build(story)
            
            logger.info(f"Successfully generated PDF report: {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Error generating PDF report: {str(e)}")
            raise
        finally:
            # Clean up temporary chart files
            self._cleanup_temp_files()
    
    def _setup_custom_styles(self):
        """Setup custom paragraph styles for the report."""
        
        # Title style
        self.styles.add(ParagraphStyle(
            name='ReportTitle',
            parent=self.styles['Title'],
            fontSize=24,
            textColor=self.colors['primary'],
            spaceAfter=30,
            alignment=TA_CENTER
        ))
        
        # Section header style
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading1'],
            fontSize=16,
            textColor=self.colors['primary'],
            spaceAfter=20,
            borderWidth=2,
            borderColor=self.colors['secondary'],
            borderPadding=10
        ))
        
        # Subsection style
        self.styles.add(ParagraphStyle(
            name='SubHeader',
            parent=self.styles['Heading2'],
            fontSize=14,
            textColor=self.colors['dark_gray'],
            spaceAfter=15
        ))
        
        # Metric highlight style
        self.styles.add(ParagraphStyle(
            name='MetricHighlight',
            parent=self.styles['Normal'],
            fontSize=18,
            textColor=self.colors['secondary'],
            alignment=TA_CENTER,
            spaceAfter=10
        ))
        
        # Footer style
        self.styles.add(ParagraphStyle(
            name='Footer',
            parent=self.styles['Normal'],
            fontSize=8,
            textColor=self.colors['dark_gray'],
            alignment=TA_CENTER
        ))
    
    def _create_title_page(
        self, 
        title: str, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any]
    ) -> List:
        """Create title page content."""
        story = []
        
        # Main title
        story.append(Paragraph(title, self.styles['ReportTitle']))
        story.append(Spacer(1, 0.5*inch))
        
        # Subtitle with period info
        current_info = current_period.get('period', {})
        comparison_info = comparison_period.get('period', {})
        
        subtitle = f"Analysis Period: {current_info.get('start_date', 'N/A')} to {current_info.get('end_date', 'N/A')}"
        story.append(Paragraph(subtitle, self.styles['SubHeader']))
        
        comparison_subtitle = f"Compared to: {comparison_info.get('start_date', 'N/A')} to {comparison_info.get('end_date', 'N/A')}"
        story.append(Paragraph(comparison_subtitle, self.styles['SubHeader']))
        
        story.append(Spacer(1, 1*inch))
        
        # Key highlights box
        highlights = self._calculate_key_highlights(current_period, comparison_period)
        story.extend(self._create_highlights_box(highlights))
        
        story.append(Spacer(1, 1*inch))
        
        # Generation info
        generation_info = f"Report generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}"
        story.append(Paragraph(generation_info, self.styles['Footer']))
        
        return story
    
    def _create_executive_summary(
        self, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any]
    ) -> List:
        """Create executive summary section."""
        story = []
        
        story.append(Paragraph("Executive Summary", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.2*inch))
        
        # Calculate key changes
        analysis = self._analyze_performance_changes(current_period, comparison_period)
        
        # Summary paragraphs
        summary_text = f"""
        This report analyzes the website performance of Taxi Drivers for the period from 
        {current_period.get('period', {}).get('start_date', 'N/A')} to 
        {current_period.get('period', {}).get('end_date', 'N/A')}, comparing it with the previous 
        equivalent period ({comparison_period.get('period', {}).get('start_date', 'N/A')} to 
        {comparison_period.get('period', {}).get('end_date', 'N/A')}).
        """
        story.append(Paragraph(summary_text, self.styles['Normal']))
        story.append(Spacer(1, 0.2*inch))
        
        # Key findings
        key_findings = self._generate_key_findings(analysis)
        story.append(Paragraph("Key Findings:", self.styles['SubHeader']))
        
        for finding in key_findings:
            story.append(Paragraph(f"• {finding}", self.styles['Normal']))
        
        return story
    
    def _create_metrics_comparison(
        self, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any]
    ) -> List:
        """Create metrics comparison section."""
        story = []
        
        story.append(Paragraph("Key Metrics Comparison", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.2*inch))
        
        # Create comparison table
        comparison_table = self._create_comparison_table(current_period, comparison_period)
        story.append(comparison_table)
        
        return story
    
    def _create_trends_analysis(self, all_periods_data: List[Dict[str, Any]]) -> List:
        """Create trends analysis section."""
        story = []
        
        story.append(Paragraph("Performance Trends", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.2*inch))
        
        if len(all_periods_data) >= 3:
            trend_analysis = self._analyze_trends(all_periods_data)
            
            for trend in trend_analysis:
                story.append(Paragraph(f"• {trend}", self.styles['Normal']))
        else:
            story.append(Paragraph("Insufficient historical data for trend analysis.", self.styles['Normal']))
        
        return story
    
    def _create_detailed_breakdown(
        self, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any]
    ) -> List:
        """Create detailed metrics breakdown."""
        story = []
        
        story.append(Paragraph("Detailed Metrics Breakdown", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.2*inch))
        
        # Traffic metrics
        story.append(Paragraph("Traffic Metrics", self.styles['SubHeader']))
        traffic_table = self._create_detailed_metrics_table(current_period, comparison_period, 'traffic')
        story.append(traffic_table)
        story.append(Spacer(1, 0.2*inch))
        
        # Engagement metrics  
        story.append(Paragraph("Engagement Metrics", self.styles['SubHeader']))
        engagement_table = self._create_detailed_metrics_table(current_period, comparison_period, 'engagement')
        story.append(engagement_table)
        
        return story
    
    def _calculate_key_highlights(
        self, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Calculate key performance highlights."""
        current_metrics = current_period.get('main_metrics', {})
        comparison_metrics = comparison_period.get('main_metrics', {})
        
        highlights = []
        
        # Page views change
        current_views = current_metrics.get('screenPageViews', 0)
        comparison_views = comparison_metrics.get('screenPageViews', 0)
        if comparison_views > 0:
            change_pct = ((current_views - comparison_views) / comparison_views) * 100
            highlights.append({
                'metric': 'Page Views',
                'current_value': f"{current_views:,.0f}",
                'change': f"{change_pct:+.1f}%",
                'trend': 'up' if change_pct > 0 else 'down'
            })
        
        # Users change
        current_users = current_metrics.get('activeUsers', 0)
        comparison_users = comparison_metrics.get('activeUsers', 0)
        if comparison_users > 0:
            change_pct = ((current_users - comparison_users) / comparison_users) * 100
            highlights.append({
                'metric': 'Active Users',
                'current_value': f"{current_users:,.0f}",
                'change': f"{change_pct:+.1f}%",
                'trend': 'up' if change_pct > 0 else 'down'
            })
        
        return highlights
    
    def _create_highlights_box(self, highlights: List[Dict[str, Any]]) -> List:
        """Create visual highlights box."""
        story = []
        
        if not highlights:
            return story
        
        # Create table for highlights
        data = [['Metric', 'Current Value', 'Change']]
        
        for highlight in highlights:
            trend_color = self.colors['success'] if highlight['trend'] == 'up' else self.colors['danger']
            data.append([
                highlight['metric'],
                highlight['current_value'],
                highlight['change']
            ])
        
        highlights_table = Table(data, colWidths=[2*inch, 1.5*inch, 1*inch])
        highlights_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
            ('TEXTCOLOR', (0, 0), (-1, 0), 'white'),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), self.colors['light_gray']),
            ('GRID', (0, 0), (-1, -1), 1, self.colors['dark_gray'])
        ]))
        
        story.append(highlights_table)
        return story
    
    def _analyze_performance_changes(
        self, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze performance changes between periods."""
        current_metrics = current_period.get('main_metrics', {})
        comparison_metrics = comparison_period.get('main_metrics', {})
        
        analysis = {}
        
        key_metrics = ['screenPageViews', 'activeUsers', 'newUsers', 'sessions', 'bounceRate', 'engagementRate']
        
        for metric in key_metrics:
            current_val = current_metrics.get(metric, 0)
            comparison_val = comparison_metrics.get(metric, 0)
            
            if comparison_val > 0:
                change_pct = ((current_val - comparison_val) / comparison_val) * 100
                change_abs = current_val - comparison_val
                
                analysis[metric] = {
                    'current': current_val,
                    'previous': comparison_val,
                    'change_percent': change_pct,
                    'change_absolute': change_abs,
                    'trend': 'increase' if change_pct > 0 else 'decrease'
                }
        
        return analysis
    
    def _generate_key_findings(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate key findings from analysis."""
        findings = []
        
        # Page views finding
        if 'screenPageViews' in analysis:
            pv_data = analysis['screenPageViews']
            if abs(pv_data['change_percent']) > 5:
                trend_word = "increased" if pv_data['trend'] == 'increase' else "decreased"
                findings.append(f"Page views {trend_word} by {abs(pv_data['change_percent']):.1f}% ({pv_data['change_absolute']:+,.0f} views)")
        
        # User engagement finding
        if 'engagementRate' in analysis:
            eng_data = analysis['engagementRate']
            if abs(eng_data['change_percent']) > 2:
                trend_word = "improved" if eng_data['trend'] == 'increase' else "declined"
                findings.append(f"User engagement {trend_word} by {abs(eng_data['change_percent']):.1f}%")
        
        # New users finding
        if 'newUsers' in analysis:
            nu_data = analysis['newUsers']
            if abs(nu_data['change_percent']) > 10:
                trend_word = "grew" if nu_data['trend'] == 'increase' else "declined"
                findings.append(f"New user acquisition {trend_word} by {abs(nu_data['change_percent']):.1f}%")
        
        return findings if findings else ["Performance metrics remained relatively stable compared to the previous period."]
    
    def _create_comparison_table(
        self, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any]
    ) -> Table:
        """Create metrics comparison table."""
        current_metrics = current_period.get('main_metrics', {})
        comparison_metrics = comparison_period.get('main_metrics', {})
        
        data = [['Metric', 'Current Period', 'Previous Period', 'Change']]
        
        metrics_info = [
            ('screenPageViews', 'Page Views', '{:,.0f}'),
            ('activeUsers', 'Active Users', '{:,.0f}'),
            ('newUsers', 'New Users', '{:,.0f}'),
            ('sessions', 'Sessions', '{:,.0f}'),
            ('bounceRate', 'Bounce Rate', '{:.1%}'),
            ('engagementRate', 'Engagement Rate', '{:.1%}'),
            ('averageSessionDuration', 'Avg Session Duration', '{:.0f}s')
        ]
        
        for metric_key, display_name, format_str in metrics_info:
            current_val = current_metrics.get(metric_key, 0)
            previous_val = comparison_metrics.get(metric_key, 0)
            
            if previous_val > 0:
                change_pct = ((current_val - previous_val) / previous_val) * 100
                change_str = f"{change_pct:+.1f}%"
            else:
                change_str = "N/A"
            
            data.append([
                display_name,
                format_str.format(current_val),
                format_str.format(previous_val),
                change_str
            ])
        
        table = Table(data, colWidths=[2*inch, 1.2*inch, 1.2*inch, 0.8*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
            ('TEXTCOLOR', (0, 0), (-1, 0), 'white'),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), self.colors['light_gray']),
            ('GRID', (0, 0), (-1, -1), 1, self.colors['dark_gray']),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [None, self.colors['light_gray']]*10)
        ]))
        
        return table
    
    def _analyze_trends(self, all_periods_data: List[Dict[str, Any]]) -> List[str]:
        """Analyze trends across all periods."""
        trends = []
        
        if len(all_periods_data) < 3:
            return trends
        
        # Sort periods by date
        sorted_periods = sorted(all_periods_data, key=lambda x: x.get('period', {}).get('start_date', ''))
        
        # Analyze page views trend
        page_views = [p.get('main_metrics', {}).get('screenPageViews', 0) for p in sorted_periods]
        if len(page_views) >= 3:
            recent_avg = sum(page_views[-3:]) / 3
            older_avg = sum(page_views[:-3]) / len(page_views[:-3]) if len(page_views) > 3 else page_views[0]
            
            if recent_avg > older_avg * 1.1:
                trends.append("Page views show an upward trend over recent periods")
            elif recent_avg < older_avg * 0.9:
                trends.append("Page views show a declining trend over recent periods")
        
        return trends
    
    def _create_detailed_metrics_table(
        self, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any], 
        category: str
    ) -> Table:
        """Create detailed metrics table for specific category."""
        # This is a simplified version - would need full implementation
        data = [['Metric', 'Current', 'Previous', 'Change']]
        data.append(['Placeholder', '0', '0', '0%'])
        
        table = Table(data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), self.colors['secondary']),
            ('TEXTCOLOR', (0, 0), (-1, 0), 'white'),
            ('GRID', (0, 0), (-1, -1), 1, self.colors['dark_gray'])
        ]))
        
        return table
    
    def _generate_charts(
        self, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any], 
        all_periods_data: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate charts and return file paths."""
        chart_paths = []
        
        try:
            # Metrics comparison chart
            chart_path = self._create_metrics_comparison_chart(current_period, comparison_period)
            if chart_path:
                chart_paths.append(chart_path)
            
            # Trend chart
            if len(all_periods_data) >= 4:
                trend_chart_path = self._create_trend_chart(all_periods_data)
                if trend_chart_path:
                    chart_paths.append(trend_chart_path)
            
        except Exception as e:
            logger.error(f"Error generating charts: {str(e)}")
        
        return chart_paths
    
    def _create_metrics_comparison_chart(
        self, 
        current_period: Dict[str, Any], 
        comparison_period: Dict[str, Any]
    ) -> Optional[str]:
        """Create metrics comparison chart."""
        try:
            current_metrics = current_period.get('main_metrics', {})
            comparison_metrics = comparison_period.get('main_metrics', {})
            
            metrics = ['screenPageViews', 'activeUsers', 'sessions']
            current_values = [current_metrics.get(m, 0) for m in metrics]
            comparison_values = [comparison_metrics.get(m, 0) for m in metrics]
            
            fig, ax = plt.subplots(figsize=(10, 6))
            
            x = range(len(metrics))
            width = 0.35
            
            ax.bar([i - width/2 for i in x], current_values, width, label='Current Period', color='#1f4e79')
            ax.bar([i + width/2 for i in x], comparison_values, width, label='Previous Period', color='#ff6b35')
            
            ax.set_xlabel('Metrics')
            ax.set_ylabel('Values')
            ax.set_title('Key Metrics Comparison')
            ax.set_xticks(x)
            ax.set_xticklabels(['Page Views', 'Active Users', 'Sessions'])
            ax.legend()
            
            plt.tight_layout()
            
            # Save chart
            chart_path = self.output_dir / 'metrics_comparison_chart.png'
            plt.savefig(chart_path, dpi=150, bbox_inches='tight')
            plt.close()
            
            return str(chart_path)
            
        except Exception as e:
            logger.error(f"Error creating comparison chart: {str(e)}")
            return None
    
    def _create_trend_chart(self, all_periods_data: List[Dict[str, Any]]) -> Optional[str]:
        """Create trend chart for all periods."""
        try:
            # Sort periods by date
            sorted_periods = sorted(all_periods_data, key=lambda x: x.get('period', {}).get('start_date', ''))
            
            dates = [p.get('period', {}).get('start_date', '') for p in sorted_periods]
            page_views = [p.get('main_metrics', {}).get('screenPageViews', 0) for p in sorted_periods]
            users = [p.get('main_metrics', {}).get('activeUsers', 0) for p in sorted_periods]
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
            
            # Page views trend
            ax1.plot(dates, page_views, marker='o', linewidth=2, color='#1f4e79')
            ax1.set_title('Page Views Trend')
            ax1.set_ylabel('Page Views')
            ax1.tick_params(axis='x', rotation=45)
            
            # Users trend
            ax2.plot(dates, users, marker='o', linewidth=2, color='#ff6b35')
            ax2.set_title('Active Users Trend')
            ax2.set_ylabel('Active Users')
            ax2.tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            # Save chart
            chart_path = self.output_dir / 'trends_chart.png'
            plt.savefig(chart_path, dpi=150, bbox_inches='tight')
            plt.close()
            
            return str(chart_path)
            
        except Exception as e:
            logger.error(f"Error creating trend chart: {str(e)}")
            return None
    
    def _add_charts_to_story(self, chart_paths: List[str]) -> List:
        """Add charts to report story."""
        story = []
        
        story.append(Paragraph("Performance Charts", self.styles['SectionHeader']))
        story.append(Spacer(1, 0.2*inch))
        
        for chart_path in chart_paths:
            if os.path.exists(chart_path):
                # Add chart image
                img = Image(chart_path, width=6*inch, height=4*inch)
                story.append(img)
                story.append(Spacer(1, 0.2*inch))
        
        return story
    
    def _cleanup_temp_files(self):
        """Clean up temporary chart files."""
        temp_files = [
            'metrics_comparison_chart.png',
            'trends_chart.png'
        ]
        
        for temp_file in temp_files:
            file_path = self.output_dir / temp_file
            if file_path.exists():
                try:
                    file_path.unlink()
                except:
                    pass  # Ignore cleanup errors