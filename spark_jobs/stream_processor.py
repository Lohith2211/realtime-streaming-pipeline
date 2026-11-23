from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, count, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder.appName("OrdersStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = (StructType()
    .add("order_id", StringType()).add("user", StringType())
    .add("category", StringType()).add("amount", DoubleType())
    .add("city", StringType()).add("event_time", StringType()))

raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load())

orders = (raw.select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", to_timestamp("event_time"))
    .filter(col("amount") > 0)
    .withWatermark("event_time", "2 minutes")
    .dropDuplicates(["order_id"]))

agg = (orders
    .groupBy(window("event_time", "1 minute"), "category")
    .agg(_sum("amount").alias("total_sales"), count("*").alias("order_count"))
    .select(col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "category", "total_sales", "order_count"))

JDBC = {"url": "jdbc:postgresql://postgres:5432/sales_db",
        "user": "de_user", "password": "de_pass",
        "driver": "org.postgresql.Driver"}

def write_pg(df, batch_id):
    df.write.format("jdbc").options(dbtable="sales_by_minute", **JDBC).mode("append").save()

(agg.writeStream
    .outputMode("update")
    .foreachBatch(write_pg)
    .option("checkpointLocation", "/checkpoints/sales")
    .start()
    .awaitTermination())