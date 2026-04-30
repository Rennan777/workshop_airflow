from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, avg

# 🔌 conexão com Spark cluster
spark = SparkSession.builder \
    .appName("bitcoin_transform") \
    .getOrCreate()

# 📥 ler do Postgres
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "bitcoin.price") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# 🔄 transformação (média por hora)
df_transformed = df \
    .withColumn("dt_hour", date_trunc("hour", col("collected_at"))) \
    .groupBy("dt_hour") \
    .agg(avg("price_brl").alias("avg_price_brl"))

# 📤 salvar de volta
df_transformed.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    .option("dbtable", "bitcoin.price_hourly") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()