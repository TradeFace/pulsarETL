package etl

import (
	"bytes"
	"fmt"

	"github.com/linkedin/goavro/v2"
	"github.com/tradeface/pulsarETL/internal/config"
)

func ProcessAvroMessage(msg []byte, transformConfig *config.MessageTransformConfig, transformedMessages chan<- []byte) error {
	// Check if message is an Avro OCF file
	if !bytes.HasPrefix(msg, []byte("Obj\x01")) {
		return fmt.Errorf("message is not an Avro OCF file")
	}

	// Create a new OCF reader for the message
	reader, err := goavro.NewOCFReader(bytes.NewReader(msg))
	if err != nil {
		return fmt.Errorf("failed to create OCF reader: %v", err)
	}

	// Get the schema from the OCF file
	schemaJSON := reader.Codec().Schema()

	// Transform the message
	transformedData, err := transformAvroData(reader, transformConfig)
	if err != nil {
		return fmt.Errorf("failed to transform message: %v", err)
	}

	// Encode the transformed message using the same schema as the original message
	encodedData, err := encodeAvroData(transformedData, schemaJSON)
	if err != nil {
		return fmt.Errorf("failed to encode transformed data: %v", err)
	}

	// Send the transformed message to the output topic
	transformedMessages <- encodedData
	return nil
}

func transformAvroData(reader *goavro.OCFReader, transformConfig *config.MessageTransformConfig) ([]map[string]interface{}, error) {
	var transformedData []map[string]interface{}

	for reader.Scan() {
		// Decode the message using the reader
		v, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read Avro message: %v", err)
		}
		record, ok := v.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to decode Avro message: expected map[string]interface{}; received %T", v)
		}
		transformedRecord := applyRecordTransformations(record, transformConfig)
		transformedData = append(transformedData, transformedRecord)
	}

	return transformedData, nil
}

func encodeAvroData(data []map[string]interface{}, schemaJSON string) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      buf,
		Schema: schemaJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create OCF encoder: %v", err)
	}

	if err := encoder.Append(data); err != nil {
		return nil, fmt.Errorf("failed to encode datum: %v", err)
	}
	return buf.Bytes(), nil
}

func applyRecordTransformations(record map[string]interface{}, transformConfig *config.MessageTransformConfig) map[string]interface{} {
	if temperature, ok := record["temperature"].(float32); ok {
		// Convert temperature from Celsius to Fahrenheit
		record["temperature"] = temperature*9.0/5.0 + 32.0
	}
	// Add additional transformations here
	return record
}
