import dlt
from pyspark.sql.functions import current_timestamp

# Paths passed from the bundle variables at runtime
landing_zone_path = spark.conf.get("bundle.landing_zone").rstrip("/")
bronze_path = spark.conf.get("bundle.bronze_path").rstrip("/")

# Sink: writes the stream as a Delta table at an external S3 path
dlt.create_sink(
    name="bronze_events_sink",
    format="delta",
    options={"path": bronze_path},
)


@dlt.append_flow(
    name="landing_to_bronze",
    target="bronze_events_sink",
    comment="Raw streaming ingestion from cloud storage using Auto Loader",
)
def landing_to_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")  # Change to json, parquet, etc. depending on source
        .option("cloudFiles.schemaLocation", f"{landing_zone_path}/_schemas")
        .option("cloudFiles.inferColumnTypes", "true")  # Automatically infer types
        .load(landing_zone_path)
        .withColumn("ingestion_time", current_timestamp())
    )
