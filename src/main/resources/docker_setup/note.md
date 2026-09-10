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
      - /home/hussain/CodeBase/ps-sql/kafka/data/upload:/home/hussain/CodeBase/ps-sql/kafka/data/upload
      - /home/hussain/CodeBase/ps-sql/kafka/data/input:/tmp/input
      - /home/hussain/CodeBase/ps-sql/kafka/data/error:/tmp/error
      - /home/hussain/CodeBase/ps-sql/kafka/data/finished:/tmp/finished 


-----------------------------------------------------------------

## if the connectors are not being created (400 Bad request)
cd to your resources/docker_setup and chage owner from root
to your current user as follow:

> cd ~/CodeBase/kafka.connect.manager/src/main/resources/docker_setup

[check current owner as follow, you will see user as root]
> ls -ld data/input data/error data/finished data/upload

[check group and id as follow (skip this step if you want)]
> docker exec -it kafka-connect id
uid=1000(appuser) gid=1000(appuser) groups=1000(appuser)

> hussain@localhost ~/CodeBase/kafka.connect.manager/src/main/resources/docker_setup λ sudo chown -R 1000:1000 data/input data/error data/finished data/upload

[Again check current owner as follow, you will see user as root]
> ls -ld data/input data/error data/finished data/upload
hussain@localhost ~/CodeBase/kafka.connect.manager/src/main/resources/docker_setup λ ls -ld data/input data/error data/finished data/upload
drwxr-xr-x 2 hussain hussain 4096 Sep 10 23:48 data/error
drwxr-xr-x 2 hussain hussain 4096 Sep 10 23:48 data/finished
drwxr-xr-x 2 hussain hussain 4096 Sep 10 23:48 data/input
drwxr-xr-x 2 hussain hussain 4096 Sep 11 00:08 data/upload

now restart the app