import dlt
from pyspark.sql.functions import current_timestamp

# Values passed from the bundle at runtime
landing_zone_path = spark.conf.get("bundle.landing_zone").rstrip("/")  # UC external volume
catalog = spark.conf.get("bundle.catalog")
schema = spark.conf.get("bundle.schema")

# Sink: writes the stream to a Unity Catalog table. Its files are stored in the
# schema's managed location (the S3 bronze path).
dlt.create_sink(
    name="bronze_events_sink",
    format="delta",
    options={"tableName": f"{catalog}.{schema}.bronze_events"},
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
