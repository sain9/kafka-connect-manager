=========================================================
KAFKA CONNECT – AFTER CHANGING CONNECTOR JSON CONFIG
=========================================================

1. Update connector JSON file
-----------------------------------------

File:
src/main/resources/connector-configs/orders-source.json

Example change:

"input.file.pattern": "orders.*\\.csv"


2. Restart Spring Boot Application
-----------------------------------------

(IntelliJ)
Stop application → Run again

OR

mvn spring-boot:run


3. Delete Existing Connector
-----------------------------------------

curl -X DELETE http://localhost:8083/connectors/orders-source


4. Verify Connector Deleted
-----------------------------------------

curl http://localhost:8083/connectors

Expected:

[]


5. Place CSV File in Input Folder
-----------------------------------------

cd ~/CodeBase/ps-sql/kafka/data/input

nano orders_001.csv


Example CSV:

order_id,customer_name,amount
1,Hussain,500
2,Ali,700
3,John,900


6. Recreate Connector via Spring Boot API
-----------------------------------------

curl -X POST \
http://localhost:8888/api/connectors/create/orders-source.json


Expected Response:

{
"name": "orders-source",
...
}


7. Verify Connector Status
-----------------------------------------

curl http://localhost:8083/connectors/orders-source/status


Expected:

"state": "RUNNING"


8. Verify Kafka Messages
-----------------------------------------

docker exec -it kafka bash


kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic orders-topic \
--from-beginning


Expected Output:

{"order_id":1,"customer_name":"Hussain","amount":500}
{"order_id":2,"customer_name":"Ali","amount":700}
{"order_id":3,"customer_name":"John","amount":900}


9. Verify File Movement
-----------------------------------------

Check input folder:

ls ~/CodeBase/ps-sql/kafka/data/input

Expected:
(empty)


Check finished folder:

ls ~/CodeBase/ps-sql/kafka/data/finished

Expected:
orders_001.csv


=========================================================
STANDARD WORKFLOW (SHORT VERSION)
=========================================================

1. Modify JSON
2. Restart Spring Boot
3. Delete connector
4. Add CSV file
5. Create connector
6. Check connector status
7. Verify Kafka messages
8. Verify file moved to finished/

=========================================================
USEFUL COMMANDS
=========================================================

Delete connector:

curl -X DELETE http://localhost:8083/connectors/orders-source


Create connector:

curl -X POST \
http://localhost:8888/api/connectors/create/orders-source.json


Check connector status:

curl http://localhost:8083/connectors/orders-source/status


List connectors:

curl http://localhost:8083/connectors


Consume Kafka topic:

docker exec -it kafka bash

kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic orders-topic \
--from-beginning


Kafka Connect logs:

docker logs kafka-connect --tail=100