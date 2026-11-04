from pyspark import pipelines as dp

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table
def sample_trips_porygon_bundle_test():
    return spark.read.table("samples.nyctaxi.trips")
