docker build -t spark-image .

docker-compose -f docker-compose.yaml -f hadoop/docker-compose.yaml -f hive/docker-compose.yaml -f spark/docker-compose.yaml up -d