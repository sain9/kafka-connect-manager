## step-1
docker compose down -v

## step-2
then run the bash script 
./setup-kafka-connect.sh

watch docker ps
re run ./setup-kafka-connect.sh if no kafka container seen 
watch docker ps

## if any issue, check the mounted directories of volume
    volumes:
      - ./data/upload:/home/hussain/CodeBase/ps-sql/kafka/data/upload
      - ./data/input:/tmp/input
      - ./data/error:/tmp/error
      - ./data/finished:/tmp/finished

