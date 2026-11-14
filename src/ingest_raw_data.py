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


@dp.table(
    name=bronze_table,
    comment="Raw CSV events ingested from the landing zone, all columns as strings",
)
def bronze_events():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")  # Change to json, parquet, etc. depending on source
        .option("cloudFiles.schemaLocation", f"{landing_zone_path}/_schemas")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("cloudFiles.cleanSource", "delete")
        .load(landing_zone_path)
    )
    # Delta rejects spaces, parentheses, etc. in column names, so normalize them
    cleaned = [clean_column(c) for c in df.columns]
    # Ensure unique names: duplicate headers after normalization get _2, _3, ...
    seen = {}
    for i, name in enumerate(cleaned):
        if name in seen:
            seen[name] += 1
            cleaned[i] = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
    df = df.toDF(*cleaned)
    return df.withColumn("ingestion_time", F.current_timestamp())
