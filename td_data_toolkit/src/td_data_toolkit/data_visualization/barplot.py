import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from typing import Dict, Optional, Tuple
from td_data_toolkit.data_transformation import pivot


class Barplot:
    """
    A class for creating customizable bar charts using `matplotlib` and `seaborn`.

    This class enables the creation of bar charts with flexible customization options, including figure size,
    axis labels, tick formatting, bar colors, and annotations.

    Attributes:
        DEFAULT_BARCHART_THEME (dict): A dictionary containing default settings for chart appearance.
        data (pd.DataFrame): The DataFrame containing the data to be plotted.
        config (dict): A configuration dictionary that overrides default settings for the plot.
    """

    DEFAULT_BARCHART_THEME: Dict = {
        "figure": {"figsize": (10, 6), "facecolor": "#000000"},
        "axes": {
            "facecolor": "#000000",
            "title": {"fontsize": 14, "color": "white"},
            "xlabel": {"fontsize": 12, "color": "white"},
            "ylabel": {"fontsize": 12, "color": "white"},
            "spines": {
                "top": False,
                "right": False,
                "left": False,
                "bottom_color": "white",
            },
            "ticks": {"color": "white", "rotation_x": 45, "align_x_ticks": "right"},
            "ylim": {"extra_margin": 0.05},  # Extra space above tallest bar
        },
        "bars": {"color": "#000000", "edgecolor": "white", "linewidth": 1.5},
        "annotations": {
            "fontsize": 12,
            "color": "white",
            "ha": "center",
            "va": "bottom",
        },
    }

    def __init__(self, data: pd.DataFrame, config: Optional[Dict] = None):
        """
        Initializes the Barplot object with a dataset and optional configuration overrides.

        :param data: DataFrame containing the data to be plotted.
        :param config: Optional dictionary with configuration overrides.
        """
        self.data = data
        self.config = config if config else self.DEFAULT_BARCHART_THEME

    def _validate_args_in_columns(self, args: Tuple[str]):
        """
        Validates if the specified columns exist in the DataFrame.

        :param args: Tuple of column names to validate.
        :raises ValueError: If any column is missing from the DataFrame.
        """
        missing_columns = [col for col in args if col not in self.data.columns]
        if missing_columns:
            raise ValueError(
                f"Columns {missing_columns} must exist in DataFrame with columns {list(self.data.columns)}"
            )

    def _plot(self, to_plot, title, x_label, y_label, perc):
        """
        Internal method to generate a bar chart with customized styling.
        """
        sns.set_theme(style="white")
        theme = self.config

        fig, ax = plt.subplots(
            figsize=theme["figure"]["figsize"], facecolor=theme["figure"]["facecolor"]
        )
        ax.set_facecolor(theme["axes"]["facecolor"])

        bars = ax.bar(
            to_plot.index,
            to_plot,
            color=theme["bars"]["color"],
            edgecolor=theme["bars"]["edgecolor"],
            linewidth=theme["bars"]["linewidth"],
        )

        for bar in bars:
            value = bar.get_height()
            label = f"{value*100:.1f}%" if perc else f"{value}"
            ax.annotate(
                label,
                (bar.get_x() + bar.get_width() / 2.0, value),
                fontsize=theme["annotations"]["fontsize"],
                color=theme["annotations"]["color"],
                ha=theme["annotations"]["ha"],
                va=theme["annotations"]["va"],
            )

        for spine, visible in theme["axes"]["spines"].items():
            if spine == "bottom_color":
                ax.spines["bottom"].set_color(visible)
            else:
                ax.spines[spine].set_visible(visible)

        ax.set_title(
            title,
            fontsize=theme["axes"]["title"]["fontsize"],
            color=theme["axes"]["title"]["color"],
        )
        ax.set_xlabel(
            x_label,
            fontsize=theme["axes"]["xlabel"]["fontsize"],
            color=theme["axes"]["xlabel"]["color"],
        )
        ax.set_ylabel(
            y_label,
            fontsize=theme["axes"]["ylabel"]["fontsize"],
            color=theme["axes"]["ylabel"]["color"],
        )

        ax.set_ylim(0, max(to_plot) + theme["axes"]["ylim"]["extra_margin"])
        ax.tick_params(
            axis="x",
            colors=theme["axes"]["ticks"]["color"],
            rotation=theme["axes"]["ticks"]["rotation_x"],
        )
        ax.tick_params(axis="y", colors=theme["axes"]["ticks"]["color"])
        ax.set_xticklabels(to_plot.index, ha=theme["axes"]["ticks"]["align_x_ticks"])
        plt.show()

    def plot_column(self, column: str, title: str = "Barplot of Counts", xlabel: str = None, ylabel: str = "Count", normalize: bool = False):
        """
        Generate a bar chart for the counts of unique values in a given column.

        :param column: The column of the DataFrame for which the counts of unique values will be plotted.
        :param title: Title of the chart (default is "Barplot of Counts").
        :param xlabel: Label for the x-axis (default is the column name).
        :param ylabel: Label for the y-axis (default is "Count").
        :param normalize: Whether to normalize the counts to proportions (default is False). If True, the counts will be shown as percentages.
        """
        # Validate that the column exists in the DataFrame
        self._validate_args_in_columns(args=(column,))
        
        # Count the occurrences of each unique value in the column
        value_counts = self.data[column].value_counts(normalize=normalize)
        
        # If normalize is True, the values are proportions (not raw counts), so format them as percentages
        if normalize:
            value_counts = value_counts * 100  # Convert to percentage

        
        # Plot the bar chart
        self._plot(
            value_counts, 
            title, 
            xlabel if xlabel else column, 
            ylabel, 
            perc=normalize  # If normalize is True, show percentages
        )


    def pivot_plot(
        self,
        by: str,
        value: str,
        ascending: bool = False,
        perc: bool = False,
        title: str = "Barchart title",
        xlabel: str = None,
        ylabel: str = None,
        agg_mode: str = "sum",
        perc_labels: bool = False,
    ):
        """
        Generate a bar chart using the pivot function for aggregation.

        :param by: Column name to group by.
        :param value: Column name containing values to aggregate.
        :param ascending: Whether to sort in ascending order.
        :param perc: The perc parameter determines whether the values should be
            shown as percentages instead of absolute numbers. When set to True, it
            calculates the percentage that each group's value contributes to the
            overall total (grand total) for the selected metric. For example, if you
            are grouping sales by region and enable perc=True, instead of showing
            total sales per region, it will display what percentage each region's-
            ales contribute to the total sales across all regions.
        :param title: Title of the chart.
        :param xlabel: Label for the x-axis (defaults to `by`).
        :param ylabel: Label for the y-axis (defaults to `value`).
        :param agg_mode: Aggregation mode ('sum' or 'mean').
        :param perc_labels: Whether to display percentage labels on the chart.
        """
        self._validate_args_in_columns(args=(by, value))
        to_plot = pivot(
            self.data, by, value, agg_mode=agg_mode, ascending=ascending, perc=perc
        )
        self._plot(
            to_plot, title, xlabel if xlabel else by, ylabel if ylabel else value, perc
        )
