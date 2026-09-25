# Databricks notebook source
#
# ShopStream - Streaming Bronze (Auto Loader)
# Picks up every new event file exactly once using Auto Loader and writes
# to the bronze_orders_stream Delta table.
#
# Source path: /Volumes/shopstream/core/events/orders_stream/
# Checkpoint:  /Volumes/shopstream/core/events/_checkpoints/bronze_orders_stream
# Target table: shopstream.core.bronze_orders_stream

# COMMAND ----------

# Auto Loader: pick up every new event file exactly once
EVENTS_PATH = "/Volumes/shopstream/core/events/orders_stream"
CHECKPOINT = "/Volumes/shopstream/core/events/_checkpoints/bronze_orders_stream"

stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT)
    .load(EVENTS_PATH))

(stream.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable("shopstream.core.bronze_orders_stream")
    .awaitTermination())

# COMMAND ----------

display(spark.sql("SELECT COUNT(*) AS events_ingested FROM shopstream.core.bronze_orders_stream"))
