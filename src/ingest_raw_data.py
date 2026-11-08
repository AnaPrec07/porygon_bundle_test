import re
import unicodedata

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def clean_column(name: str) -> str:
    """Turn e.g. 'Fecha transacción' / 'Cargos (Db)' into 'fecha_transaccion' / 'cargos_db'."""
    if name.startswith("_"):  # keep Auto Loader's _rescued_data as is
        return name
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()  # drop accents
    return re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()


# Values passed from the bundle at runtime
landing_zone_path = spark.conf.get("bundle.landing_zone").rstrip("/")  # UC external volume
bronze_table = f"{spark.conf.get('bundle.catalog')}.{spark.conf.get('bundle.schema')}.bronze_events"

# Target streaming table. The flow below appends into it.
dp.create_streaming_table(
    name=bronze_table,
    comment="Raw CSV events ingested from the landing zone, all columns as strings",
)


@dp.append_flow(
    target=bronze_table,
    name="landing_to_bronze",
    comment="Raw streaming ingestion from cloud storage using Auto Loader",
)
@dp.expect("no rescued data", "_rescued_data IS NULL")
def landing_to_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")  # Change to json, parquet, etc. depending on source
        .option("cloudFiles.schemaLocation", f"{landing_zone_path}/_schemas")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("cloudFiles.cleanSource", "delete")
        .load(landing_zone_path)
    )
    # Delta rejects spaces, parentheses, etc. in column names, so normalize them
    df = df.toDF(*[clean_column(c) for c in df.columns])
    return df.withColumn("ingestion_time", F.current_timestamp())
