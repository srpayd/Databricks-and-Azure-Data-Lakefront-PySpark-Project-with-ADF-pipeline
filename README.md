# Databricks & Azure Data Lakefront: PySpark Innovation Project


This project is designed as a hands-on exploration of advanced data processing and analytics technologies, with a focus on building and optimizing an ETL pipeline. The goal is to gain practical experience with Azure Databricks, PySpark for distributed computing, and Azure Data Lake Storage for handling large-scale data. An important aspect of this project is working with Parquet files, a semi-structured columnar format ideal for optimized data storage. The ETL pipeline is structured for bulk loading efficiency, and the outcome of the data processing is visualized using Power BI, offering insights through rich and interactive dashboards.

#### Built With:
- Azure Databricks
- PySpark & SparkSQL
- Distributed Computing
- Azure Data Lake Storage
- Parquet file (semi-structured data & columnar format)
- ETL Pipeline: (Tool: Azure Data Factory, Pipeline: Batch Processing, Loading Type: Incremental Loading)
- Visualization: Power BI
  
## I. Solution Architecture Overview <be>

This diagram represents the solution architecture for the project's data processing workflow. It starts with data extraction through an API, then moves to raw and staged data layers in Azure Data Lake, transformation through Azure Databricks, and finally, analysis and reporting in Power BI.


![image](https://github.com/srpayd/Databricks-Spark---Azure-Data-Lake/assets/39004568/5c0f4136-ace2-44d1-ae3b-7e8cabe92293)


## II. Getting Started

#### a. About the Data
The Ergast API provides a comprehensive database of Formula 1 data, including information on circuits, races, drivers, constructors, qualifying sessions, lap times, pit stops, and much more. Our project aims to leverage this rich dataset to uncover insights, analyze trends, and visualize key metrics related to Formula 1 racing.

![image](https://github.com/srpayd/Databricks-Spark-Project-using-Azure-Data-Lake/assets/39004568/662b8239-2e29-4e99-a481-507ff2401142)

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
<br><br>
#### d. Scheduling Requirements 

- Schedule to run every Sunday 10PM
- Ability to monitor pipelines
- Ability to re-run failed pipelines
- Ability to set-up alerts on failures
<br><br>
#### e. Misc Requirements 

- Ability to see history and time travel
- Ability to roll back to a previous version 


## IV. Usage

### Pre-requisites

Before setting up the project, ensure you have the following tools and accounts set up:
- Azure subscription
- Azure Databricks workspace
- Azure Storage account with Data Lake enabled
- Power BI account

## Contact

Serap Aydogdu - [LinkedIn](https://www.linkedin.com/in/srpayd/) | [Medium](https://medium.com/@srpayd)

## Reference

This study benefits from the "Azure Databricks & Spark For Data Engineers (PySpark / SQL)" online course on Udemy. [Link](https://www.linkedin.com/in/srpayd/](https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/?couponCode=KEEPLEARNING)https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/?couponCode=KEEPLEARNING)  





