import re
import unicodedata

import dlt
from pyspark.sql.functions import current_timestamp


def clean_column(name: str) -> str:
    """Turn e.g. 'Fecha transacción' / 'Cargos (Db)' into 'fecha_transaccion' / 'cargos_db'."""
    if name.startswith("_"):  # keep Auto Loader's _rescued_data as is
        return name
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()  # drop accents
    return re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()


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
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")  # Change to json, parquet, etc. depending on source
        .option("cloudFiles.schemaLocation", f"{landing_zone_path}/_schemas")
        .option("cloudFiles.inferColumnTypes", "true")  # Automatically infer types
        .load(landing_zone_path)
    )
    # Delta rejects spaces, parentheses, etc. in column names, so normalize them
    df = df.toDF(*[clean_column(c) for c in df.columns])
    return df.withColumn("ingestion_time", current_timestamp())
