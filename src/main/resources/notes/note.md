# how to use this project:

-- place the csv file in upload directory
-- a connector will be created based on the csv, and will be saved in the src/main/resources/connector-configs directory
-- then the connector will be registered in kafka connect
-- csv file will be moved to input directory
-- the registered connector will be watching the input directory and when the csv moved here,connector
will process the csv file and push the data in the kafka topic (as per the connector configuration)
-- then the file will be moved in the finished directory