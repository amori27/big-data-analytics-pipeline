# Big Data Analytics Pipeline

PySpark ETL pipeline with Delta Lake — reads from S3/GCS, applies DataFrame transformations and aggregations, writes to data warehouses and data lakes.

## Usage

```python
from src.pipeline import DataPipeline
pipeline = DataPipeline(spark_master="local[*]")
pipeline.load_data("s3://bucket/data.csv")
pipeline.transform(...)
pipeline.save("s3://bucket/output/")
```

## License

MIT
