
# Chicago Crashes Data Modeling
## Overview
This project aims to process data crawled from Soda API related to crashes happended in Chicago and conduct analysis from processed data.

## **Tools & Technology**
- Hadoop
- Docker
- Apache Spark
- Delta Lake
- Apache Hive
- Apache Airflow
- Apache Superset
- Language: Python

## Data Architecture
![architecture](images/DataFlowChicagoCrash.jpg)

The project was built by multiple services deployed by Docker. Spark was used to ingest data to Data Lakehouse managed by delta lake hosted on HDFS and transform raw data to star schema stored in Data Warehouse used for data analysis. Superset was used for data visualization and data analysis. Finally, airflow was used to manage and schedule processing workflow happening in this project.

## Data Model

The image below is visualized result star schema:

![datamodel](images/Chicago%20Car%20Crash%20Diagram.jpg)

This star schema use Mini Dimension to manage rapidly change dimension such as condition of traffic control divice and road condition. Bridge table was used to manage dimensions that have M:N relationship with fact table such as Vehicle Dimension or Person Dimension.

## Example
The example visualization from data warehouse after transform from raw data:

![Visualization](images/chicago-crashes-dashboard-2023-11-02T05-34-39.256Z.jpg)