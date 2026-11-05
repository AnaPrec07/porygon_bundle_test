import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--export-path", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()

export_path = args.export_path.rstrip("/")

# Incremental export: the checkpoint remembers what was already exported, so each
# run only writes rows added to bronze_events since the previous run.
(
    spark.readStream.table(f"{args.catalog}.{args.schema}.bronze_events")
    .writeStream.format("parquet")  # change to "delta", "csv", "json" if needed
    .option("checkpointLocation", f"{export_path}/_checkpoints/bronze_events")
    .trigger(availableNow=True)
    .start(f"{export_path}/bronze_events")
    .awaitTermination()
)
