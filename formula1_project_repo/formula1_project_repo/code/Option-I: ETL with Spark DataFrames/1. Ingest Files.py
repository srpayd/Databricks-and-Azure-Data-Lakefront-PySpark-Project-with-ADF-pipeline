# Databricks notebook source
# MAGIC %md
# MAGIC # Formula 1 Data Analysis 

# COMMAND ----------

# MAGIC %md
# MAGIC ### About the Data
# MAGIC
# MAGIC The Ergast API provides a comprehensive database of Formula 1 data, including information on circuits, races, drivers, constructors, qualifying sessions, lap times, pit stops, and much more. Our project aims to leverage this rich dataset to uncover insights, analyze trends, and visualize key metrics related to Formula 1 racing.
# MAGIC
# MAGIC ### Project Goals
# MAGIC Gain insights into the performance of drivers and constructors across different seasons and races.
# MAGIC Analyze race results, qualifying performances, and championship standings.
# MAGIC Explore circuit characteristics, lap times, and strategic pit stop data.
# MAGIC Visualize key metrics to enhance understanding and facilitate data-driven decision-making.
# MAGIC
# MAGIC ### Approach:
# MAGIC We will adopt a data-driven approach to analyze Formula 1 data, leveraging Python and PySpark for data manipulation, analysis, and visualization. By querying the Ergast API, we can retrieve relevant datasets and transform them into structured formats suitable for analysis. We will use various statistical techniques, visualization libraries, and machine learning algorithms to extract insights and patterns from the data.

# COMMAND ----------

# MAGIC %md
# MAGIC #### The structure of the database entity Relationship Diagram <br><br>
# MAGIC
# MAGIC <img src="https://ergast.com/images/ergast_db.png" alt="Your Image">

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Project Requirements 
# MAGIC
# MAGIC ##### I. Data Ingesting Requirements 
# MAGIC
# MAGIC - Ingest All 8 files into Azure Data Lake
# MAGIC - Ingest data must have the schema applied
# MAGIC - Ingested data must have audit columns 
# MAGIC - Ingested data must be stored in columnar format 
# MAGIC - Must be able to analyze the ingested data via SQL
# MAGIC - Ingestion logic must be able to handle incremental load
# MAGIC <br><br>
# MAGIC ##### II. Data Transfromation Requirements 
# MAGIC
# MAGIC - Join the key information required for reporting to create a new table
# MAGIC - Join the key information required for Analysis to create a new table
# MAGIC - Transformed table must have audit columns 
# MAGIC - Must be able to analyze the transformed data viw SQL
# MAGIC - Transformed data must be stored in columnar format
# MAGIC - Transformation logic must be able to handle incremental load
# MAGIC <br><br>
# MAGIC ##### III. BI Reporting Requirements 
# MAGIC
# MAGIC - Provide a comprehensive overview of Formula 1 racing, including historical insights and recent trends.
# MAGIC - Uncover patterns and correlations within the data to better understand the factors influencing race outcomes i.e. dominant drivers, dominant teams
# MAGIC - Create interactive Databricks visualizations and dashboards to showcase our findings and engage with the Formula 1 community.
# MAGIC - Explore opportunities for further research and analysis in the field of motorsport analytics.
# MAGIC
# MAGIC ##### VI. Scheduling Requirements 
# MAGIC
# MAGIC - Schedule to run every Sunday 10PM
# MAGIC - Ability to monitor pipelines
# MAGIC - Ability to re-run failed pipelines
# MAGIC - Ability to set-up alerts on failures
# MAGIC
# MAGIC
# MAGIC ##### V. Misc Requirements 
# MAGIC
# MAGIC - Ability to see history and time travel
# MAGIC - Ability to roll back to a previous version 

# COMMAND ----------

# MAGIC %run "../include/config"

# COMMAND ----------

# MAGIC %run "../include/common_functions"

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

circuits_df = spark.read.csv(f"{raw_folder_path}/circuits.csv", header=True, schema=circuits_schema)
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

circuits_final_df = add_ingestion_date(circuits_selected_df) \
                                        .withColumn('env', lit('production'))                     
circuits_final_df.show()

# COMMAND ----------

# Write Spark DataFrame to Parquet

circuits_final_df.write.parquet(f"{stage_folder_path}/circuits", mode='overwrite')

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

races_df = spark.read.csv(f"{raw_folder_path}/races.csv", header=True, schema=races_schema)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(races_df['date'], lit(' '), races_df['time']), 'yyyy-MM-dd HH:mm:ss'))


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
                                                   races_with_timestamp_df['race_timestamp'])


# COMMAND ----------

# MAGIC %md
# MAGIC ###### **Let's write partitoned races DataFrame & store it into Azure Data Lake Storage "stage" container. This will make easier Spark to read the data if you're looking only a data with a specif date & helps Spark to process the data in paralel

# COMMAND ----------

races_selected_df.write.partitionBy('race_year').parquet(f"{stage_folder_path}/races", mode='overwrite')

# COMMAND ----------

display(dbutils.fs.ls(stage_folder_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## constructor.json

# COMMAND ----------

# DDL based schema definition
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_folder_path}/constructors.json", schema = constructors_schema)

# COMMAND ----------



# COMMAND ----------

# Transformations

### Drop a column
constructor_dropped_df = constructor_df.drop(constructor_df['url'])

### Rename & New Column
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructor_final_df.write.parquet(f"{stage_folder_path}/constructors", mode='overwrite')

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

drivers_df = spark.read.json(f"{raw_folder_path}/drivers.json", schema = drivers_schema)

# COMMAND ----------

# Transformation

from pyspark.sql.functions import col, concat, current_timestamp, lit

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(drivers_df["name"].forename, lit(" "), drivers_df["name"].surname))

drivers_final_df = drivers_with_columns_df.drop(drivers_with_columns_df["url"])

# COMMAND ----------

drivers_final_df.write.parquet(f"{stage_folder_path}/drivers", mode='overwrite')
display(dbutils.fs.ls(stage_folder_path))

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

results_df = spark.read.json(f"{raw_folder_path}/results.json", schema=results_schema )

# COMMAND ----------

# Transformation
from pyspark.sql.functions import current_timestamp

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) 


results_final_df = results_with_columns_df.drop(results_with_columns_df["statusId"])


# COMMAND ----------

results_final_df.write.partitionBy('race_id').parquet(f"{stage_folder_path}/results", mode="overwrite")
display(dbutils.fs.ls(stage_folder_path))

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

pit_stops_df = spark.read.json(f"{raw_folder_path}/pit_stops.json" , multiLine=True, schema=pit_stops_schema)

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# Transformation

from pyspark.sql.functions import current_timestamp

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
                       .withColumnRenamed("raceId", "race_id") \
                       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{stage_folder_path}/pit_stops")
display(dbutils.fs.ls(stage_folder_path))

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
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{stage_folder_path}/lap_times")
display(dbutils.fs.ls(stage_folder_path))

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
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{stage_folder_path}/qualifying")
display(dbutils.fs.ls(stage_folder_path))

# COMMAND ----------

dbutils.notebook.exit('Success')
