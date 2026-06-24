"""Aggregators Module.

This module provides aggregation and window function
capabilities for PySpark DataFrames.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Any


class DataAggregator:
    """Handles aggregations and window functions."""

    @staticmethod
    def aggregate(
        df: DataFrame,
        group_by: list[str],
        aggregations: dict[str, str]
    ) -> DataFrame:
        """Aggregate DataFrame by group.

        Args:
            df: Input DataFrame.
            group_by: Columns to group by.
            aggregations: Dict of column:agg_function.

        Returns:
            Aggregated DataFrame.
        """
        agg_exprs = []
        for col, func in aggregations.items():
            if func == "sum":
                agg_exprs.append(F.sum(col).alias(col))
            elif func == "avg":
                agg_exprs.append(F.avg(col).alias(col))
            elif func == "count":
                agg_exprs.append(F.count(col).alias(col))
            elif func == "min":
                agg_exprs.append(F.min(col).alias(col))
            elif func == "max":
                agg_exprs.append(F.max(col).alias(col))

        return df.groupBy(*group_by).agg(*agg_exprs)

    @staticmethod
    def running_total(
        df: DataFrame,
        partition_by: list[str] | None,
        order_by: str,
        column: str,
        new_column: str = "running_total"
    ) -> DataFrame:
        """Calculate running total.

        Args:
            df: Input DataFrame.
            partition_by: Columns to partition by.
            order_by: Column to order by.
            column: Column to sum.
            new_column: Output column name.

        Returns:
            DataFrame with running total.
        """
        window_spec = Window.partitionBy(*partition_by).orderBy(order_by) if partition_by else Window.orderBy(order_by)
        return df.withColumn(new_column, F.sum(column).over(window_spec))

    @staticmethod
    def moving_average(
        df: DataFrame,
        partition_by: list[str] | None,
        order_by: str,
        column: str,
        window_size: int,
        new_column: str = "moving_avg"
    ) -> DataFrame:
        """Calculate moving average.

        Args:
            df: Input DataFrame.
            partition_by: Columns to partition by.
            order_by: Column to order by.
            column: Column to average.
            window_size: Window size.
            new_column: Output column name.

        Returns:
            DataFrame with moving average.
        """
        window_spec = Window.partitionBy(*partition_by).orderBy(order_by).rowsBetween(-window_size, 0) if partition_by else Window.orderBy(order_by).rowsBetween(-window_size, 0)
        return df.withColumn(new_column, F.avg(column).over(window_spec))

    @staticmethod
    def rank(
        df: DataFrame,
        partition_by: list[str] | None,
        order_by: str,
        new_column: str = "rank"
    ) -> DataFrame:
        """Add rank column.

        Args:
            df: Input DataFrame.
            partition_by: Columns to partition by.
            order_by: Column to order by.
            new_column: Output column name.

        Returns:
            DataFrame with rank.
        """
        window_spec = Window.partitionBy(*partition_by).orderBy(order_by) if partition_by else Window.orderBy(order_by)
        return df.withColumn(new_column, F.rank().over(window_spec))

    @staticmethod
    def dense_rank(
        df: DataFrame,
        partition_by: list[str] | None,
        order_by: str,
        new_column: str = "dense_rank"
    ) -> DataFrame:
        """Add dense rank column.

        Args:
            df: Input DataFrame.
            partition_by: Columns to partition by.
            order_by: Column to order by.
            new_column: Output column name.

        Returns:
            DataFrame with dense rank.
        """
        window_spec = Window.partitionBy(*partition_by).orderBy(order_by) if partition_by else Window.orderBy(order_by)
        return df.withColumn(new_column, F.dense_rank().over(window_spec))

    @staticmethod
    def lag_column(
        df: DataFrame,
        partition_by: list[str] | None,
        order_by: str,
        column: str,
        offset: int = 1,
        new_column: str | None = None
    ) -> DataFrame:
        """Add lag column.

        Args:
            df: Input DataFrame.
            partition_by: Columns to partition by.
            order_by: Column to order by.
            column: Column to lag.
            offset: Lag offset.
            new_column: Output column name.

        Returns:
            DataFrame with lagged column.
        """
        col_name = new_column or f"{column}_lag"
        window_spec = Window.partitionBy(*partition_by).orderBy(order_by) if partition_by else Window.orderBy(order_by)
        return df.withColumn(col_name, F.lag(column, offset).over(window_spec))

    @staticmethod
    def lead_column(
        df: DataFrame,
        partition_by: list[str] | None,
        order_by: str,
        column: str,
        offset: int = 1,
        new_column: str | None = None
    ) -> DataFrame:
        """Add lead column.

        Args:
            df: Input DataFrame.
            partition_by: Columns to partition by.
            order_by: Column to order by.
            column: Column to lead.
            offset: Lead offset.
            new_column: Output column name.

        Returns:
            DataFrame with leading column.
        """
        col_name = new_column or f"{column}_lead"
        window_spec = Window.partitionBy(*partition_by).orderBy(order_by) if partition_by else Window.orderBy(order_by)
        return df.withColumn(col_name, F.lead(column, offset).over(window_spec))


def create_summary_report(df: DataFrame, group_col: str, value_col: str) -> DataFrame:
    """Create summary report with multiple aggregations.

    Args:
        df: Input DataFrame.
        group_col: Column to group by.
        value_col: Value column to aggregate.

    Returns:
        Summary DataFrame.
    """
    return df.groupBy(group_col).agg(
        F.count(value_col).alias("count"),
        F.sum(value_col).alias("total"),
        F.avg(value_col).alias("average"),
        F.min(value_col).alias("minimum"),
        F.max(value_col).alias("maximum"),
        F.stddev(value_col).alias("stddev")
    )
