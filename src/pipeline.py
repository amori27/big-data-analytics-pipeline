"""Data Pipeline Module.

This module provides the main PySpark-based data pipeline
for ETL operations on big data.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Any


class DataPipeline:
    """Main data pipeline orchestrator."""

    def __init__(
        self,
        app_name: str = "DataPipeline",
        spark_master: str = "local[*]"
    ):
        """Initialize the DataPipeline.

        Args:
            app_name: Application name.
            spark_master: Spark master URL.
        """
        self.app_name = app_name
        self.spark_master = spark_master
        self.spark = None
        self.dataframe: DataFrame | None = None

    def start(self) -> SparkSession:
        """Start Spark session.

        Returns:
            SparkSession instance.
        """
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.spark_master) \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        return self.spark

    def stop(self) -> None:
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            self.spark = None

    def load_csv(
        self,
        path: str,
        header: bool = True,
        infer_schema: bool = True
    ) -> DataFrame:
        """Load CSV file.

        Args:
            path: File path.
            header: Whether CSV has header.
            infer_schema: Infer schema automatically.

        Returns:
            DataFrame.
        """
        if not self.spark:
            self.start()

        self.dataframe = self.spark.read.csv(
            path,
            header=header,
            inferSchema=infer_schema
        )
        return self.dataframe

    def load_json(self, path: str) -> DataFrame:
        """Load JSON file.

        Args:
            path: File path.

        Returns:
            DataFrame.
        """
        if not self.spark:
            self.start()

        self.dataframe = self.spark.read.json(path)
        return self.dataframe

    def load_parquet(self, path: str) -> DataFrame:
        """Load Parquet file.

        Args:
            path: File path.

        Returns:
            DataFrame.
        """
        if not self.spark:
            self.start()

        self.dataframe = self.spark.read.parquet(path)
        return self.dataframe

    def transform(
        self,
        func: callable,
        *args: Any,
        **kwargs: Any
    ) -> DataFrame:
        """Apply transformation function.

        Args:
            func: Transformation function.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Transformed DataFrame.
        """
        if self.dataframe is None:
            raise ValueError("No data loaded. Call load_* first.")

        self.dataframe = func(self.dataframe, *args, **kwargs)
        return self.dataframe

    def save_parquet(self, path: str, mode: str = "overwrite") -> None:
        """Save DataFrame as Parquet.

        Args:
            path: Output path.
            mode: Write mode.
        """
        if self.dataframe:
            self.dataframe.write.parquet(path, mode=mode)

    def save_csv(self, path: str, mode: str = "overwrite", header: bool = True) -> None:
        """Save DataFrame as CSV.

        Args:
            path: Output path.
            mode: Write mode.
            header: Include header.
        """
        if self.dataframe:
            self.dataframe.write.csv(path, mode=mode, header=header)

    def save_json(self, path: str, mode: str = "overwrite") -> None:
        """Save DataFrame as JSON.

        Args:
            path: Output path.
            mode: Write mode.
        """
        if self.dataframe:
            self.dataframe.write.json(path, mode=mode)

    def execute_sql(
        self,
        query: str,
        view_name: str = "temp_view"
    ) -> DataFrame:
        """Execute SQL query on DataFrame.

        Args:
            query: SQL query.
            view_name: Temporary view name.

        Returns:
            Query result DataFrame.
        """
        if self.dataframe is None:
            raise ValueError("No data loaded.")

        self.dataframe.createOrReplaceTempView(view_name)
        return self.spark.sql(query)


def create_pipeline(
    app_name: str = "DataPipeline",
    config: dict[str, Any] | None = None
) -> DataPipeline:
    """Factory function to create pipeline.

    Args:
        app_name: Application name.
        config: Optional configuration.

    Returns:
        DataPipeline instance.
    """
    pipeline = DataPipeline(app_name=app_name)
    return pipeline
