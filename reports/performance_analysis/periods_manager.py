"""
Historical Periods Manager for Performance Analysis

This module handles calculation of historical periods for comparative analysis,
following data engineering best practices for date handling and period management.
"""

import logging
from datetime import datetime, timedelta, date
from typing import List, Tuple, Dict, Optional
import calendar

logger = logging.getLogger(__name__)


class HistoricalPeriodsManager:
    """
    Manages calculation of historical periods for performance comparison.
    
    Features:
    - Automatic calculation of equivalent historical periods
    - Configurable minimum date boundaries
    - Smart period alignment (same day of week, etc.)
    - Comprehensive validation and error handling
    """
    
    def __init__(self, min_date: str = "2025-01-01"):
        """
        Initialize the historical periods manager.
        
        Args:
            min_date: Earliest date to consider (YYYY-MM-DD format)
        """
        self.min_date = datetime.strptime(min_date, '%Y-%m-%d').date()
        logger.info(f"Initialized HistoricalPeriodsManager with min_date={min_date}")
    
    def calculate_historical_periods(
        self, 
        start_date: str, 
        end_date: str,
        max_periods: Optional[int] = None
    ) -> List[Dict[str, str]]:
        """
        Calculate all historical periods of the same duration.
        
        Args:
            start_date: Target period start date (YYYY-MM-DD)
            end_date: Target period end date (YYYY-MM-DD)
            max_periods: Maximum number of historical periods to return
            
        Returns:
            List of period dictionaries with start_date, end_date, and metadata
        """
        logger.info(f"Calculating historical periods for {start_date} to {end_date}")
        
        try:
            # Validate and parse dates
            target_start = datetime.strptime(start_date, '%Y-%m-%d').date()
            target_end = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            # Calculate period duration
            period_duration = (target_end - target_start).days + 1
            logger.info(f"Period duration: {period_duration} days")
            
            # Generate all historical periods
            periods = []
            current_end = target_start - timedelta(days=1)  # Start from day before target period
            
            period_counter = 0
            while current_end >= self.min_date and (max_periods is None or period_counter < max_periods):
                current_start = current_end - timedelta(days=period_duration - 1)
                
                # Check if this period is valid (not before min_date)
                if current_start >= self.min_date:
                    period_info = {
                        'start_date': current_start.strftime('%Y-%m-%d'),
                        'end_date': current_end.strftime('%Y-%m-%d'),
                        'duration_days': period_duration,
                        'period_type': self._classify_period_type(current_start, current_end),
                        'days_before_target': (target_start - current_end).days,
                        'period_label': self._generate_period_label(current_start, current_end)
                    }
                    
                    periods.append(period_info)
                    period_counter += 1
                    
                    logger.debug(f"Added historical period: {period_info['start_date']} to {period_info['end_date']}")
                else:
                    # If we've hit the minimum date boundary, stop
                    break
                
                # Move to next historical period
                current_end = current_start - timedelta(days=1)
            
            logger.info(f"Generated {len(periods)} historical periods")
            return periods
            
        except Exception as e:
            logger.error(f"Error calculating historical periods: {str(e)}")
            raise
    
    def get_all_analysis_periods(
        self, 
        start_date: str, 
        end_date: str,
        max_periods: Optional[int] = None
    ) -> List[Dict[str, str]]:
        """
        Get all periods for analysis: target period + all historical periods.
        
        Args:
            start_date: Target period start date (YYYY-MM-DD)
            end_date: Target period end date (YYYY-MM-DD)
            max_periods: Maximum number of total periods to return
            
        Returns:
            List of all periods with target period first, then historical periods
        """
        try:
            # Target period
            target_start = datetime.strptime(start_date, '%Y-%m-%d').date()
            target_end = datetime.strptime(end_date, '%Y-%m-%d').date()
            period_duration = (target_end - target_start).days + 1
            
            target_period = {
                'start_date': start_date,
                'end_date': end_date,
                'duration_days': period_duration,
                'period_type': self._classify_period_type(target_start, target_end),
                'days_before_target': 0,
                'period_label': self._generate_period_label(target_start, target_end),
                'is_target_period': True
            }
            
            # Historical periods
            max_historical = max_periods - 1 if max_periods else None
            historical_periods = self.calculate_historical_periods(
                start_date, end_date, max_historical
            )
            
            # Mark historical periods
            for period in historical_periods:
                period['is_target_period'] = False
            
            # Combine and return
            all_periods = [target_period] + historical_periods
            
            logger.info(f"Generated {len(all_periods)} total analysis periods")
            return all_periods
            
        except Exception as e:
            logger.error(f"Error getting all analysis periods: {str(e)}")
            raise
    
    def _classify_period_type(self, start_date: date, end_date: date) -> str:
        """Classify the type of period based on duration and alignment."""
        duration = (end_date - start_date).days + 1
        
        # Check for common period types
        if duration == 1:
            return "daily"
        elif duration == 7:
            return "weekly"
        elif 28 <= duration <= 31:
            # Check if it spans a full month
            if (start_date.day == 1 and 
                end_date.day == calendar.monthrange(end_date.year, end_date.month)[1]):
                return "monthly"
            else:
                return "monthly_custom"
        elif 89 <= duration <= 92:
            return "quarterly"
        elif 364 <= duration <= 366:
            return "yearly"
        else:
            return "custom"
    
    def _generate_period_label(self, start_date: date, end_date: date) -> str:
        """Generate a human-readable label for the period."""
        try:
            duration = (end_date - start_date).days + 1
            
            if duration == 1:
                return start_date.strftime("%B %d, %Y")
            elif duration == 7:
                return f"Week of {start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
            elif start_date.month == end_date.month:
                return f"{start_date.strftime('%B %d')} - {end_date.strftime('%d, %Y')}"
            else:
                return f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
                
        except Exception as e:
            logger.warning(f"Error generating period label: {str(e)}")
            return f"{start_date} to {end_date}"
    
    def validate_period_alignment(
        self, 
        periods: List[Dict[str, str]]
    ) -> Dict[str, bool]:
        """
        Validate that all periods have consistent alignment and duration.
        
        Args:
            periods: List of period dictionaries
            
        Returns:
            Dictionary with validation results
        """
        validation_results = {
            'consistent_duration': True,
            'valid_date_ranges': True,
            'no_overlaps': True,
            'min_date_compliance': True
        }
        
        try:
            if not periods:
                return validation_results
            
            # Check consistent duration
            durations = [p['duration_days'] for p in periods]
            if len(set(durations)) > 1:
                validation_results['consistent_duration'] = False
                logger.warning("Periods have inconsistent durations")
            
            # Check valid date ranges and min_date compliance
            for period in periods:
                start_dt = datetime.strptime(period['start_date'], '%Y-%m-%d').date()
                end_dt = datetime.strptime(period['end_date'], '%Y-%m-%d').date()
                
                if start_dt > end_dt:
                    validation_results['valid_date_ranges'] = False
                    logger.error(f"Invalid date range: {period['start_date']} to {period['end_date']}")
                
                if start_dt < self.min_date:
                    validation_results['min_date_compliance'] = False
                    logger.warning(f"Period starts before min_date: {period['start_date']}")
            
            # Check for overlaps
            sorted_periods = sorted(periods, key=lambda x: x['start_date'])
            for i in range(len(sorted_periods) - 1):
                current_end = datetime.strptime(sorted_periods[i]['end_date'], '%Y-%m-%d').date()
                next_start = datetime.strptime(sorted_periods[i + 1]['start_date'], '%Y-%m-%d').date()
                
                if current_end >= next_start:
                    validation_results['no_overlaps'] = False
                    logger.error(f"Overlapping periods detected")
                    break
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating period alignment: {str(e)}")
            # Return all False on validation error
            return {key: False for key in validation_results.keys()}
    
    def get_comparison_pairs(
        self, 
        periods: List[Dict[str, str]]
    ) -> List[Tuple[Dict[str, str], Dict[str, str]]]:
        """
        Generate meaningful comparison pairs from periods list.
        
        Args:
            periods: List of period dictionaries (should be sorted with most recent first)
            
        Returns:
            List of tuples with (recent_period, comparison_period) pairs
        """
        comparison_pairs = []
        
        try:
            if len(periods) < 2:
                logger.warning("Insufficient periods for comparison")
                return comparison_pairs
            
            # Main comparison: most recent vs second most recent
            if len(periods) >= 2:
                comparison_pairs.append((periods[0], periods[1]))
            
            # Additional comparisons if more periods available
            if len(periods) >= 4:
                # Recent vs 3 periods ago (monthly comparison if weekly data)
                comparison_pairs.append((periods[0], periods[3]))
            
            if len(periods) >= 12:
                # Recent vs 12 periods ago (yearly comparison if monthly data) 
                comparison_pairs.append((periods[0], periods[11]))
            
            logger.info(f"Generated {len(comparison_pairs)} comparison pairs")
            return comparison_pairs
            
        except Exception as e:
            logger.error(f"Error generating comparison pairs: {str(e)}")
            return []