import pandas as pd

def pivot(data: pd.DataFrame, by: str, value: str, agg_mode: str = "sum", ascending: bool = None, perc: bool = False):
    """
    Perform a pivot operation using the specified aggregation mode.
    
    :param data: DataFrame containing the data.
    :param by: Column name to group by.
    :param value: Column name containing values to aggregate.
    :param agg_mode: Aggregation mode ('sum' or 'mean'), defaults to 'sum'.
    :param ascending: Sort order, defaults to None (no sorting).
    :param perc: Whether to return values as percentages (only applies to sum), defaults to False.
    :return: Aggregated DataFrame.
    """
    if agg_mode not in {"sum", "mean"}:
        raise ValueError("Unsupported aggregation mode. Choose 'sum' or 'mean'.")

    grouped = data.groupby(by=by)[value].agg(agg_mode)

    if agg_mode == "sum" and perc:
        grouped /= grouped.sum()

    if ascending is not None:
        grouped = grouped.sort_values(ascending=ascending)

    return grouped

