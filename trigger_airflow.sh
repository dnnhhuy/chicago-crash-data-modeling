#!/bin/bash
docker exec airflow-worker airflow dags unpause 'api_crawler'
docker exec airflow-worker airflow dags unpause 'etl_pipeline'