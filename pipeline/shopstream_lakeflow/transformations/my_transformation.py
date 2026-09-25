import dlt

@dlt.table(
    name="pipe_events_bronze",
    comment="Raw order events from the live feed"
)
def pipe_events_bronze():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/shopstream/core/events/orders_stream")
    )
