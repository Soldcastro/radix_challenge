from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import traceback

print('Starting process...')

# Spark Configuration
print('Setting up Spark...')
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("airflow_app") \
    .config('spark.executor.memory', '6g') \
    .config('spark.driver.memory', '6g') \
    .config("spark.driver.maxResultSize", "1048MB") \
    .config("spark.port.maxRetries", "100") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("INFO")


# Variables
print('Setting Variables...')
source_path = 'datalake/trusted'
write_path = 'datalake/refined'
days = 7
days_in_sec = lambda i: i * 86400
partition = 1
partition_fields = ['ano']


# Read files do Dataframe
print('Reading Files to Dataframe...')
try:
    trusted_df = spark.read.format("parquet") \
        .load(source_path)
except Exception:
    print(traceback.format_exc())


# Window for rolling_avg
print("Calculating rolling average")
window_spec = (Window.partitionBy("pais").orderBy(F.col("data").cast('long')).rangeBetween(-days_in_sec(6), 0))


# Columns with rolling_avg
trusted_df = trusted_df.withColumn("media_movel_confirmados", F.avg("quantidade_confirmados").over(window_spec))
trusted_df = trusted_df.withColumn("media_movel_mortes", F.avg("quantidade_mortes").over(window_spec))
trusted_df = trusted_df.withColumn("media_movel_recuperados", F.avg("quantidade_recuperados").over(window_spec))


# Creates Refined Dataframe with required columns
print('Creating Dataframe for writing...')
refined_df = trusted_df.select("pais",
                               "data",
                               "media_movel_confirmados",
                               "media_movel_mortes",
                               "media_movel_recuperados",
                               "ano")


# Datatype corrections
print('Correcting datatypes...')
refined_df = refined_df.withColumn("media_movel_confirmados", F.col("media_movel_confirmados").cast("long"))
refined_df = refined_df.withColumn("media_movel_mortes", F.col("media_movel_mortes").cast("long"))
refined_df = refined_df.withColumn("media_movel_recuperados", F.col("media_movel_recuperados").cast("long"))


# Forcing 1 partition to write 1 file
refined_df = refined_df.repartition(partition)


# Writing Parquet Files
print('Writing parquet files...')
try:
    refined_df.write \
    .mode('overwrite') \
    .format('parquet') \
    .partitionBy(partition_fields) \
    .option("parquet.compress", "snappy") \
    .save(write_path)
except Exception:
    print(traceback.format_exc())

print('Process End...')
