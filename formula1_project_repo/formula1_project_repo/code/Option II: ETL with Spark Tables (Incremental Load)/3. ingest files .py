# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Files into Managed Tables 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Data Ingesting Requirements 
# MAGIC
# MAGIC - Ingest All 8 files into Azure Data Lake
# MAGIC - Ingest data must have the schema applied
# MAGIC - Ingested data must have audit columns 
# MAGIC - Ingested data must be stored in columnar format 
# MAGIC - Must be able to analyze the ingested data via SQL
# MAGIC - Ingestion logic must be able to handle incremental load
# MAGIC <br><br>
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# default is our first historical folder with named 2021-03-21.
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../../includes/config"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

# Let's list all data we have in Azure Data Lake Storage 

dbutils.fs.ls(raw_folder_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## circuits.csv

# COMMAND ----------

# Identify schema

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields = [StructField('circuitId', IntegerType(), False),
                                       StructField('circuitRef', StringType(), True),
                                       StructField('name', StringType(), True),
                                       StructField('location', StringType(), True),
                                       StructField('country', StringType(), True),
                                       StructField('lat', DoubleType(), True),
                                       StructField('lng', DoubleType(), True),
                                       StructField('alt', IntegerType(), True),
                                       StructField('url', StringType(), True)
])

# COMMAND ----------

# Reading CSV files from Raw Container into Stage Container from ADLs.

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header=True, schema=circuits_schema)
circuits_df.dtypes

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

# Filter columns & alias them
circuits_selected_df = circuits_df.select(circuits_df.circuitId.alias('circuit_id'), 
                                          circuits_df.circuitRef.alias('circuit_ref'), 
                                          circuits_df.name, 
                                          circuits_df.location, 
                                          circuits_df.country, 
                                          circuits_df.lat.alias('latitude'), 
                                          circuits_df.lng.alias('longitude'), 
                                          circuits_df.alt.alias('altitude'))
circuits_selected_df.show()


# COMMAND ----------


# Adding a new audit column : ingestion_date
circuits_selected_df = circuits_df.select(circuits_df.circuitId.alias('circuit_id'), 
                                          circuits_df.circuitRef.alias('circuit_ref'), 
                                          circuits_df.name, 
                                          circuits_df.location, 
                                          circuits_df.country, 
                                          circuits_df.lat.alias('latitude'), 
                                          circuits_df.lng.alias('longitude'), 
                                          circuits_df.alt.alias('altitude'))
circuits_selected_df.show()


# COMMAND ----------


# Adding a new audit column : ingestion_date : it reusable function under include Notebook. 
# Adding a new literal column : environment

from pyspark.sql.functions import current_timestamp, lit

circuits_final_df = add_ingestion_date(circuits_selected_df, v_file_date) \
                    .withColumn('env', lit('production'))

             
circuits_final_df.show()

# COMMAND ----------

# Write Spark DataFrame as a parquet formatted managed table into DBFS 
# and also into location where we specified in DDL statement of formuladl101_processed 

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_processed.circuits")


# COMMAND ----------

display(dbutils.fs.ls(stage_folder_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## races.csv

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
                                  ])


# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", header=True, schema=races_schema)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(races_df['date'], lit(' '), races_df['time']), 'yyyy-MM-dd HH:mm:ss')) \
                                  .withColumn('file_date', lit(v_file_date)) 


# COMMAND ----------

# MAGIC %md
# MAGIC to_timestamp(string, format)

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(races_with_timestamp_df['raceId'].alias('race_id'), 
                                                   races_with_timestamp_df['year'].alias('race_year'), 
                                                   races_with_timestamp_df['round'], 
                                                   races_with_timestamp_df['circuitId'].alias('circuit_id'),
                                                   races_with_timestamp_df['name'], 
                                                   races_with_timestamp_df['ingestion_date'], 
                                                   races_with_timestamp_df['race_timestamp'],
                                                   races_with_timestamp_df['file_date'])


# COMMAND ----------

races_selected_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### **Let's write partitoned races DataFrame & store it into Azure Data Lake Storage "stage" container. This will make easier Spark to read the data if you're looking only a data with a specif date & helps Spark to process the data in paralel

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("formuladl101_processed.races")

# COMMAND ----------

display(dbutils.fs.ls(stage_folder_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## constructor.json

# COMMAND ----------

# DDL based schema definition
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json", schema = constructors_schema)

# COMMAND ----------

# Transformations

### Drop a column
constructor_dropped_df = constructor_df.drop(constructor_df['url'])

### Rename & New Column
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn('file_date', lit(v_file_date)) 

# COMMAND ----------

constructor_final_df.show()

# COMMAND ----------

# constructor_final_df.write.parquet(f"{stage_folder_path}/constructors", mode='overwrite')

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_processed.constructors")


# COMMAND ----------

display(dbutils.fs.ls(stage_folder_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## drivers.json 

# COMMAND ----------

# MAGIC %md
# MAGIC ----> Inner json {a: 10, b: {x:100, y:200} , c: 'serap'}

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# name is a inner object. So we need this schema inference as well

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json", schema = drivers_schema)

# COMMAND ----------

# Transformation

from pyspark.sql.functions import col, concat, current_timestamp, lit

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(drivers_df["name"].forename, lit(" "), drivers_df["name"].surname)) \
                                    .withColumn('file_date', lit(v_file_date)) 

drivers_final_df = drivers_with_columns_df.drop(drivers_with_columns_df["url"])

# COMMAND ----------

drivers_final_df.show()

# COMMAND ----------

# drivers_final_df.write.parquet(f"{stage_folder_path}/drivers", mode='overwrite')

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("formuladl101_processed.drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## results.json   

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json", schema=results_schema )

# COMMAND ----------

# Transformation
from pyspark.sql.functions import current_timestamp, lit

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn('file_date', lit(v_file_date))  

results_final_df = results_with_columns_df.drop(results_with_columns_df["statusId"])


# COMMAND ----------

output_df = re_arrange_partition_column(results_final_df, 'race_id')

# COMMAND ----------

overwrite_partition(output_df, 'formuladl101_processed', 'results', 'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT race_id, COUNT(*)
# MAGIC FROM formuladl101_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## pit_stops.json   ---> Multiple Lines : jsonb[]

# COMMAND ----------

# MAGIC %md
# MAGIC ----> multi line json :     [ {a: 10, b: 20}  ,  {a: 66, b: 44}  ,  {a: 1111, b: 2222} ]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])


# COMMAND ----------

pit_stops_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json" , multiLine=True, schema=pit_stops_schema)

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# Transformation

from pyspark.sql.functions import current_timestamp

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                       .withColumnRenamed("raceId", "race_id") \
                       .withColumn("ingestion_date", current_timestamp()) \
                       .withColumn('file_date', lit(v_file_date))  

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

output_df = re_arrange_partition_column(final_df, 'race_id')

# COMMAND ----------

overwrite_partition(output_df, 'formuladl101_processed', 'pit_stops', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ## lap_times.json   ---> Multiple Lines : jsonb[]
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])


# COMMAND ----------

lap_times_df = spark.read \
                    .schema(lap_times_schema) \
                    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumn("ingestion_date", current_timestamp()) \
                        .withColumn('file_date', lit(v_file_date))  

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

output_df = re_arrange_partition_column(final_df, 'race_id')

# COMMAND ----------

overwrite_partition(output_df, 'formuladl101_processed', 'lap_times', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ## qualifying.json  
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
                    .schema(qualifying_schema) \
                    .option("multiLine", True) \
                    .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumn("ingestion_date", current_timestamp()) \
                        .withColumn('file_date', lit(v_file_date))  

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

output_df = re_arrange_partition_column(final_df, 'race_id')

# COMMAND ----------

overwrite_partition(output_df, 'formuladl101_processed', 'qualifying', 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
