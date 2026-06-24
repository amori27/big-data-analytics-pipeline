"""Data Transformers Module.

This module provides common PySpark data transformations
for cleaning, enrichment, and reshaping data.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Any


class DataTransformer:
    """Handles common data transformations."""

    @staticmethod
    def clean_nulls(
        df: DataFrame,
        strategy: str = "drop",
        fill_value: dict[str, Any] | None = None
    ) -> DataFrame:
        """Handle null values in DataFrame.

        Args:
            df: Input DataFrame.
            strategy: Strategy (drop, fill, mean, median).
            fill_value: Dict of column:value for filling.

        Returns:
            Cleaned DataFrame.
        """
        if strategy == "drop":
            return df.dropna()
        elif strategy == "fill" and fill_value:
            for col, value in fill_value.items():
                df = df.fillna({col: value})
            return df
        return df

    @staticmethod
    def remove_duplicates(df: DataFrame, subset: list[str] | None = None) -> DataFrame:
        """Remove duplicate rows.

        Args:
            df: Input DataFrame.
            subset: Columns to consider for duplicates.

        Returns:
            DataFrame without duplicates.
        """
        return df.dropDuplicates(subset=subset)

    @staticmethod
    def add_derived_column(
        df: DataFrame,
        column_name: str,
        expression: str
    ) -> DataFrame:
        """Add a derived column.

        Args:
            df: Input DataFrame.
            column_name: Name for new column.
            expression: SQL expression.

        Returns:
            DataFrame with new column.
        """
        return df.withColumn(column_name, F.expr(expression))

    @staticmethod
    def rename_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
        """Rename columns.

        Args:
            df: Input DataFrame.
            mapping: Dict of old_name: new_name.

        Returns:
            DataFrame with renamed columns.
        """
        for old_name, new_name in mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

    @staticmethod
    def filter_rows(
        df: DataFrame,
        condition: str
    ) -> DataFrame:
        """Filter rows based on condition.

        Args:
            df: Input DataFrame.
            condition: Filter condition.

        Returns:
            Filtered DataFrame.
        """
        return df.filter(condition)

    @staticmethod
    def normalize_column(
        df: DataFrame,
        column: str,
        new_column: str | None = None
    ) -> DataFrame:
        """Normalize a numeric column.

        Args:
            df: Input DataFrame.
            column: Column to normalize.
            new_column: Optional new column name.

        Returns:
            DataFrame with normalized column.
        """
        col_name = new_column or f"{column}_normalized"

        stats = df.select(
            F.mean(column).alias("mean"),
            F.stddev(column).alias("stddev")
        ).collect()[0]

        mean_val = stats["mean"]
        std_val = stats["stddev"]

        if std_val and std_val > 0:
            df = df.withColumn(
                col_name,
                (F.col(column) - mean_val) / std_val
            )
        return df

    @staticmethod
    def bucket_column(
        df: DataFrame,
        column: str,
        num_buckets: int,
        new_column: str = "bucket"
    ) -> DataFrame:
        """Bucket a numeric column.

        Args:
            df: Input DataFrame.
            column: Column to bucket.
            num_buckets: Number of buckets.
            new_column: Bucket column name.

        Returns:
            DataFrame with bucket column.
        """
        return df.withColumn(
            new_column,
            F.floor(F.col(column) * num_buckets)
        )

    @staticmethod
    def parse_json_column(
        df: DataFrame,
        column: str,
        schema: Any | None = None
    ) -> DataFrame:
        """Parse JSON string column.

        Args:
            df: Input DataFrame.
            column: JSON column name.
            schema: Optional schema.

        Returns:
            DataFrame with parsed JSON.
        """
        parsed = df.withColumn("parsed", F.from_json(F.col(column), schema or "string"))
        return df.withColumn(column, F.col("parsed")).drop("parsed")


def apply_transformation_pipeline(
    df: DataFrame,
    transformations: list[dict[str, Any]]
) -> DataFrame:
    """Apply a series of transformations.

    Args:
        df: Input DataFrame.
        transformations: List of transformation configs.

    Returns:
        Transformed DataFrame.
    """
    transformer = DataTransformer()

    for transform in transformations:
        transform_type = transform.get("type")
        params = transform.get("params", {})

        if transform_type == "clean_nulls":
            df = transformer.clean_nulls(df, **params)
        elif transform_type == "remove_duplicates":
            df = transformer.remove_duplicates(df, **params)
        elif transform_type == "filter":
            df = transformer.filter_rows(df, **params)
        elif transform_type == "add_derived":
            df = transformer.add_derived_column(df, **params)

    return df
