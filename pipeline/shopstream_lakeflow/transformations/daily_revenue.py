import dlt
from pyspark.sql.functions import to_date, expr

@dlt.table(
    name="pipe_daily_revenue",
    comment="Daily revenue from the live event stream"
)
def pipe_daily_revenue():

    return (
        dlt.read("pipe_events_bronze")
        .where("status = 'completed'")
        .groupBy(
            to_date("event_ts").alias("event_date")
        )
        .agg(
            expr("count(*) AS events"),
            expr("round(sum(quantity * unit_price), 2) AS revenue")
        )
    )
