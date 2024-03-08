# Distributed Data Systems Project: Scalable Data Aggregation

## Team Members
- Tatshini Ganesan
- Caleb Hamblen
- Rishi Mohan
- Yi-Fang Tsai

## Course Details
MSDS697 Distributed Data Systems, instructed by Prof. Mahesh Chaudhari

## Project Overview
The primary aim of our project was to demonstrate the integration and orchestration of various cloud computing resources for distributed data processing. We focused on creating a seamless pipeline that integrates Google Cloud services, MongoDB Atlas, and Apache Spark, with Airflow DAGs managing the scheduling and execution of tasks. Our goal was to mimic a real-world cloud machine learning deployment, emphasizing the orchestration and interaction between cloud services rather than on model tuning.

## Project Goals
- **Integration**: Leverage Google Cloud's BigQuery for data warehousing, Dataproc for Spark, Cloud Composer for Airflow, and MongoDB Atlas as a staging database.
- **Orchestration**: Utilize Airflow DAGs to automate the data processing workflow, ensuring efficient and timely data handling.
- **Real-world Simulation**: Implement a cloud-based machine learning model deployment focusing on data processing and analysis over model accuracy tuning.

## Datasets Utilized
We employed three significant datasets from BigQuery's public records, focusing on San Francisco's municipal service data:
1. **San Francisco 311 Service Requests**: Records from July 2008 to present, detailing non-emergency municipal service requests. Updated daily.
2. **SFPD Crime Incident Reporting**: Data from January 2003 until 2018, documenting incidents reported to the San Francisco Police Department.
3. **SF Fire Department Service Calls**: Fire unit responses from April 2000 to present, updated daily.

## Data Processing Pipeline
Our pipeline is structured into two main components:
1. **Data Retrieval**: Automated extraction of updated data from BigQuery, utilizing Airflow DAGs to manage the insertion of new data into MongoDB Atlas without duplications.
2. **Data Processing and Analysis**: Leveraging PySpark on Dataproc for aggregations and modeling, with tasks scheduled to reflect the distinct computational demands of real-world data processing.

### DAGs (Directory: `dags/`)
Our Airflow Directed Acyclic Graphs (DAGs) are at the core of orchestrating the data workflow. We have two primary DAGs:
- **Data Ingestion DAG**: Manages daily updates from BigQuery to MongoDB Atlas, ensuring new data is captured without duplicating existing records.
- **Modeling and Aggregation DAG**: Scheduled less frequently, this DAG triggers PySpark jobs on Dataproc to perform data analysis and model retraining, accounting for the computational expense associated with these tasks.

The DAGs are designed to facilitate a seamless flow of data from extraction to analysis, enabling efficient data management and processing.

### Spark Jobs (Directory: `spark_jobs/`)
We implemented several Spark jobs to handle data processing and analysis:
- **Crime Statistics Aggregation**: Analyzes crime incidents data to generate yearly and street-level statistics.
- **Service Request Analysis**: Processes 311 service requests to identify trends and common issues over time.
- **Fire Department Call Analysis**: Utilizes data from fire department calls to forecast emergency service demand using machine learning models.

These jobs demonstrate the power of Spark in processing large datasets and the utility of cloud-based resources in handling complex data analysis tasks.

## Analytics and Aggregations
Key insights from our data analysis include trends in auto thefts, the prevalence of certain types of service requests, and forecasting of emergency service demands. These insights are crucial for municipal planning and resource allocation.

## Performance and Observations
Our analysis offered a comparison between BigQuery and MongoDB Atlas, highlighting the strengths and limitations of each in handling complex queries. Despite some latency issues with MongoDB Atlas, its flexibility and scalability were beneficial for our project needs.

## Conclusion
This project successfully demonstrated the capabilities of cloud computing services in managing and analyzing large datasets. Through the integration of Google Cloud services, MongoDB Atlas, and Spark, orchestrated by Airflow, we gained valuable insights into distributed data processing and cloud computing technologies. Our work lays a foundation for future projects and professional applications in the field of distributed data systems.
