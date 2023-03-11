Pulsar ETL
===================

reads a pulsar topic (Avro OCF format). Does a simple transformation of temperature. Produces a new Avro OCF message and send it to another plusar topic. 

- go run cmd/produce/main.go 
  - creates a Avro OCF message with 6 rows and writes it to my_input_topic
- go run cmd/etl/main.go
  - reads a Avro OCF message from my_input_topic
  - unpacks the 6 rows 
  - transforms the temperature from celcius to farenheit
  - repacks the rows in a new Avro OCF message 
  - writes the new message to my_output_topic
- go run cmd/consume/main.go
  - reads a Avro OCF message from my_output_topic
  - unpacks the 6 rows and displays them
