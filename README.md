
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

## **Setup & Deployment**

### Prerequisites
- Docker

### Setup
I prepared few scripts to setup and start the project automatically. In order to setup this project, the following steps are required for the project to be successfully executed.

* Firstly, you need to gain permission for shell scripts by using the following command:
    ```
    chmod +x *.sh
    ```

* Then run the following command to setup required images and containers:
    ```
    ./setup.sh
    ```
    This function is downloading/buidling required images, then creating containers for services used in this project. </br>
    
* In order to start processing streaming and batch events, use this script:
    ```
    ./trigger-airflow.sh
    ```
    This script start scraping data from airbnb and store data into HDFS as format of delta table.

* To shut down the project, use this command:
    ```
    ./shutdown.sh
    ```

* Running services can be directly accessible at following these sites:
    * Spark Web UI: http://localhost:8080
    * HDFS Web UI: http://localhost:9870 (You can observe scraped data in this directory /spark-warehouse/{table} on HDFS)
    * Superset WebUI: http://localhost:8089