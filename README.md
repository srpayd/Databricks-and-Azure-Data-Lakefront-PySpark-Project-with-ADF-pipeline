# Databricks-Spark-Project-using-Azure-Data-Lake


....Short description of your project, what it does, and what problem it solves.

## I. Solution Architecture Overview <be>



![image](https://github.com/srpayd/Databricks-Spark---Azure-Data-Lake/assets/39004568/5c0f4136-ace2-44d1-ae3b-7e8cabe92293)

### Built With 
- Azure Databrick 
- PySpark
- Distributed Computing
- Azure Data Lake Storage
- Parquet file (columnar format)
- ETL pipeline: Bulk Loading
- Visualization: Power BI  

## II. Getting Started

#### a. About the Data
The Ergast API provides a comprehensive database of Formula 1 data, including information on circuits, races, drivers, constructors, qualifying sessions, lap times, pit stops, and much more. Our project aims to leverage this rich dataset to uncover insights, analyze trends, and visualize key metrics related to Formula 1 racing.

#### b. Project Goals
Gain insights into the performance of drivers and constructors across different seasons and races. Analyze race results, qualifying performances, and championship standings. Explore circuit characteristics, lap times, and strategic pit stop data. Visualize key metrics to enhance understanding and facilitate data-driven decision-making.

#### c. Approach
We will adopt a data-driven approach to analyze Formula 1 data, leveraging Python and PySpark for data manipulation, analysis, and visualization. By querying the Ergast API, we can retrieve relevant datasets and transform them into structured formats suitable for analysis. We will use various statistical techniques, visualization libraries, and machine learning algorithms to extract insights and patterns from the data.

## III. Project Requirements 

#### a. Data Ingesting Requirements 

- Ingest All 8 files into Azure Data Lake
- Ingest data must have the schema applied
- Ingested data must have audit columns 
- Ingested data must be stored in columnar format 
- Must be able to analyze the ingested data via SQL
- Ingestion logic must be able to handle incremental load
<br><br>
#### b. Data Transfromation Requirements 

- Join the key information required for reporting to create a new table
- Join the key information required for Analysis to create a new table
- Transformed table must have audit columns 
- Must be able to analyze the transformed data viw SQL
- Transformed data must be stored in columnar format
- Transformation logic must be able to handle incremental load
<br><br>
#### c. BI Reporting Requirements 

- Provide a comprehensive overview of Formula 1 racing, including historical insights and recent trends.
- Uncover patterns and correlations within the data to better understand the factors influencing race outcomes i.e. dominant drivers, dominant teams
- Create interactive Databricks visualizations and dashboards to showcase our findings and engage with the Formula 1 community.
- Explore opportunities for further research and analysis in the field of motorsport analytics.

#### d. Scheduling Requirements 

- Schedule to run every Sunday 10PM
- Ability to monitor pipelines
- Ability to re-run failed pipelines
- Ability to set-up alerts on failures


#### e. Misc Requirements 

- Ability to see history and time travel
- Ability to roll back to a previous version 



These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

## IV. Prerequisites

What things you need to install the software and how to install them.



