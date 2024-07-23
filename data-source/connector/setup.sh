curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @connector/local/source-debezium.json

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @connector/local/sink-s3.json

##
curl http://localhost:8083/connectors/orders-source/status
curl http://localhost:8083/connectors/orders-sink/status

#### TEST
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @connector/local/source-debezium-avro.json

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
  http://localhost:8083/connectors/ -d @connector/local/source-debezium-simple.json

curl http://localhost:8083/connectors/orders-source-avro/status
curl http://localhost:8083/connectors/orders-source-simple/status


curl http://localhost:8081/subjects
# ["avro.datalake.cdc_events-key","avro.datalake.cdc_events-value"]
