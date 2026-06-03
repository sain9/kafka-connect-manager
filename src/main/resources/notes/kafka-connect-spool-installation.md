Manual setup for spooldir , use setup-kafka-connect.sh instead
# 1. Go into the Kafka Connect container
docker exec -it kafka-connect bash

# 2. Inside the container, run these commands:
confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:latest

# 3. Verify installation
ls -la /usr/share/confluent-hub-components/

# 4. Exit container
exit

# 5. Restart Kafka Connect
docker restart kafka-connect

# 6. Wait 30 seconds
sleep 30

# 7. Check if plugin appears
curl -s http://localhost:8083/connector-plugins | grep -i spool