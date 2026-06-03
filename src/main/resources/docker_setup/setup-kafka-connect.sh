# Start Docker services
docker compose up -d

# Wait for Kafka Connect to start
sleep 30

# Install the plugin manually inside the container
docker exec -it kafka-connect bash -c "
confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:2.0.43 || \
confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:2.0.35 || \
confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:2.0.30
"

# Restart Kafka Connect to load the plugin
docker restart kafka-connect

# Wait for restart
sleep 30

# Verify plugin is installed
curl -s http://localhost:8083/connector-plugins | python3 -m json.tool | grep -i spool -A 3