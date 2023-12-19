# libraries
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


# variables
print('Setting Variables...')
read_path = 'datalake/raw/covid19'
write_path = 'datalake/trusted'
partition_fields = ['ano', 'mes']
partition = 1


# frame read
print('Reading Files to Dataframe...')
try:
    df = spark.read.format("csv") \
        .options(header='true', delimiter=',') \
        .load(read_path)
except Exception:
    print(traceback.format_exc())


# transformations
# unpivoting date columns
print('Starting transformations')
cols_to_unpivot = [f"`{c}`, \'{c}\'" for c in df.columns if c not in ['Province/State', 'Country/Region', 'Lat', 'Long']]
stack_string = ", ".join(cols_to_unpivot)

df_unpivot = df.select(
    'Province/State', 'Country/Region', 'Lat', 'Long',
    F.expr(f"stack({len(cols_to_unpivot)}, {stack_string}) as (Cases, Date)")
)


# Converting dates to yyyy-mm-dd format
df_unpivot = df_unpivot.withColumn('Date', F.to_timestamp('Date', 'M/d/yy'))


# adding Source file name to dataframe for future filter
df_unpivot = df_unpivot.withColumn('Source_file', F.input_file_name())


# adding Source column based on Source_file for better reading and dropping Source_file
df_unpivot = df_unpivot.withColumn('Source', F.when(F.col('Source_file').contains('confirmed'), 'confirmed')
                                                .when(F.col('Source_file').contains('deaths'), 'deaths')
                                                .when(F.col('Source_file').contains('recovered'), 'recovered')) \
                        .drop('Source_file')

# Creating windown to get the case increase for each type
window_spec = Window.partitionBy('Country/Region','Source').orderBy('Date')


# Adding column Cases_increase with only the new cases increase by day.
df_unpivot = df_unpivot.withColumn('Cases_increase', F.col("Cases") - F.lag("Cases", 1, 0).over(window_spec))


# Renaming coluns for easier SQL
df_unpivot = df_unpivot.withColumnRenamed('Province/State', 'Province_State') \
                    .withColumnRenamed('Country/Region', 'Country_Region')

print("Transformations Ended...")
# Creating trusted dataframe for writing
print('Creating Dataframe for writing...')
df_unpivot.createOrReplaceTempView('raw_table')
trusted_sql = ("select Country_Region as pais, \
               Province_State as estado, \
               Lat as latitude, \
               Long as longitude, \
               Date as data, \
               case when Source == 'confirmed' then Cases_increase end as quantidade_confirmados, \
               case when Source == 'deaths' then Cases_increase end as quantidade_mortes, \
               case when Source == 'recovered' then Cases end as quantidade_recuperados \
               from raw_table \
               ")

try:
    df_trusted = spark.sql(trusted_sql)
except Exception:
    print(traceback.format_exc())


# Adding partition columns
print('Adding partition columns and correcting datatypes...')
df_trusted = df_trusted.withColumn("ano", F.year("data")) \
                .withColumn("mes", F.month("data"))

# Forcing datatypes
df_trusted = df_trusted.withColumn('latitude', F.col('latitude').cast("double"))
df_trusted = df_trusted.withColumn('longitude', F.col('longitude').cast("double"))
df_trusted = df_trusted.withColumn('quantidade_confirmados', F.col('quantidade_confirmados').cast("long"))
df_trusted = df_trusted.withColumn('quantidade_mortes', F.col('quantidade_mortes').cast("long"))
df_trusted = df_trusted.withColumn('quantidade_recuperados', F.col('quantidade_recuperados').cast("long"))


# Forcing 1 partition to write 1 file
df_trusted = df_trusted.repartition(partition)


# Writing Parquet Files
print('Writing parquet files...')
df_trusted.write.mode('overwrite') \
    .format('parquet') \
    .partitionBy(partition_fields) \
    .option("parquet.compress", "snappy") \
    .save(write_path)


print('Process End...')