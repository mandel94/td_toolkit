import pandas as pd
from typing import Optional, Union, Callable, Tuple
from datetime import datetime, timedelta

def standardize_to_midnight(dt: datetime) -> datetime:
    """
    Standardizes a datetime object to midnight (00:00:00).

    :param dt: The datetime object to standardize.
    :type dt: datetime

    :returns: The datetime object standardized to midnight.
    :rtype: datetime
    """
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

def ensure_timezone_aware(dt: Optional[datetime], timezone: str = 'UTC') -> Optional[datetime]:
    """
    Ensures that a datetime object is timezone-aware. If it is naive, it will be localized to the specified timezone.

    :param dt: The datetime object to check and convert.
    :type dt: Optional[datetime]
    :param timezone: The target timezone to localize naive datetime objects.
    :type timezone: str

    :returns: A timezone-aware datetime object.
    :rtype: Optional[datetime]
    """
    if dt and dt.tzinfo is None:
        return pd.Timestamp(dt).tz_localize(timezone)
    return dt

def convert_column_to_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Ensures that the specified column is converted to datetime dtype.

    :param df: The DataFrame containing the column to convert.
    :param column: The name of the column to convert to datetime.

    :returns: The DataFrame with the specified column converted to datetime.
    :rtype: pd.DataFrame
    """
    if not pd.api.types.is_datetime64_any_dtype(df[column]):
        df[column] = pd.to_datetime(df[column])
    return df

def convert_to_utc(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Converts the datetime column to UTC.

    :param df: The DataFrame containing the datetime column to convert.
    :param column: The name of the datetime column to convert.
    
    :returns: The DataFrame with the datetime column converted to UTC.
    :rtype: pd.DataFrame
    """
    # Convert to UTC if the datetime column is not already localized
    if df[column].dt.tz is None:
        df[column] = df[column].dt.tz_localize('UTC')
    
    return df

def convert_to_timezone(df: pd.DataFrame, column: str, timezone: Optional[str]) -> pd.DataFrame:
    """
    Converts the datetime column to the specified timezone, if a timezone is provided.

    :param df: The DataFrame containing the datetime column to convert.
    :param column: The name of the datetime column to convert.
    :param timezone: The target timezone string (e.g., "UTC", "US/Eastern").
    :type timezone: Optional[str]
    
    :returns: The DataFrame with the datetime column converted to the specified timezone.
    :rtype: pd.DataFrame
    """
    # If a timezone is provided, convert the datetime column to the target timezone
    if timezone:
        df[column] = df[column].dt.tz_convert(timezone)
    
    return df

def get_last_week_range(standardize: bool = True) -> Tuple[datetime, datetime]:
    """
    Returns a tuple representing the datetime range for the last 7 full days (excluding today).

    :param standardize: Whether to standardize the start and end dates to midnight (00:00:00).
    :type standardize: bool

    :returns: (start_date, end_date)
    :rtype: Tuple[datetime, datetime]
    """
    today = datetime.today()
    end = today - timedelta(days=1)
    start = end - timedelta(days=6)

    # Standardize to midnight if needed
    if standardize:
        start = standardize_to_midnight(start)
        end = standardize_to_midnight(end)

    return start, end


def filter_by_datetime(
    df: pd.DataFrame,
    column: str,
    date_filter: Union[
        datetime,
        Tuple[Optional[datetime], Optional[datetime]],
        Callable[[pd.Series], pd.Series],
        str,
    ],
    timezone: Optional[str] = None,  # New parameter to define the target timezone
    standardize: bool = True  # Standardize datetime to midnight if True
) -> pd.DataFrame:
    """
    Filters a DataFrame based on a datetime column using various types of filters.

    All datetime values are normalized to **day granularity** (i.e., the time component is removed).
    This means comparisons are made at the date level (e.g., 2025-01-01 14:00 and 2025-01-01 00:00 are treated as equal).

    :param df: The DataFrame to be filtered.
    :type df: pd.DataFrame

    :param column: The name of the datetime column to filter on.
    :type column: str

    :param date_filter: The filter to apply. This can be:
        - A single datetime object for exact date match.
        - A tuple (start_datetime, end_datetime) for a date range. Either value can be None for open-ended ranges.
        - A callable (e.g., lambda function) that accepts a datetime Series and returns a boolean mask.
        - A string to filter by special keywords ("last_week" => past 7 days (excluding today))
    :type date_filter: Union[datetime, Tuple[Optional[datetime], Optional[datetime]], Callable[[pd.Series], pd.Series]]

    :param timezone: The target timezone string (e.g., "UTC", "US/Eastern").
    :type timezone: Optional[str]

    :param standardize: Whether to standardize the datetime values to midnight (00:00:00).
    :type standardize: bool

    :returns: A filtered DataFrame based on the normalized date values.
    :rtype: pd.DataFrame

    :raises ValueError: If the column is not of datetime type.
    :raises TypeError: If the filter is not a supported type.
    """
    # Ensure the datetime column is properly converted to datetime type
    df = convert_column_to_datetime(df, column)

    if not pd.api.types.is_datetime64_any_dtype(df[column]):
        raise ValueError(f"Column '{column}' must be of datetime dtype.")

    # Convert the datetime column to UTC first (if needed)
    df = convert_to_utc(df, column)

    # Convert the datetime column to the specified timezone (if provided)
    df = convert_to_timezone(df, column, timezone)

    # Standardize datetime to midnight if needed
    if standardize:
        df[column] = df[column].apply(standardize_to_midnight)

    if isinstance(date_filter, str):
        if date_filter == "last_week":
            start, end = get_last_week_range(standardize)
            start = ensure_timezone_aware(start, 'UTC')
            end = ensure_timezone_aware(end, 'UTC')
            return df[(df[column] >= start) & (df[column] <= end)]
        else:
            raise ValueError(f"Unknown date filter keyword: {date_filter}")

    if isinstance(date_filter, datetime):
        # Normalize input datetime as well
        date_filter = standardize_to_midnight(date_filter) if standardize else date_filter
        date_filter = ensure_timezone_aware(date_filter, 'UTC')
        return df[df[column] == date_filter]

    elif isinstance(date_filter, tuple):
        start, end = date_filter
        if start:
            start = standardize_to_midnight(start) if standardize else start
            start = ensure_timezone_aware(start, 'UTC')
        if end:
            end = standardize_to_midnight(end) if standardize else end
            end = ensure_timezone_aware(end, 'UTC')

        if start and end:
            return df[(df[column] >= start) & (df[column] <= end)]
        elif start:
            return df[df[column] >= start]
        elif end:
            return df[df[column] <= end]
        else:
            return df

    elif callable(date_filter):
        return df[date_filter(df[column])]

    else:
        raise TypeError("Unsupported date_filter type.")
