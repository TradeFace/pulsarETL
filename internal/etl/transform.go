package etl

import (
	"bytes"
	"fmt"

	"github.com/linkedin/goavro/v2"
	"github.com/tradeface/pulsarETL/internal/config"
)

type FailedMessage struct {
	Data   []map[string]interface{}
	Errors []error
}

func ProcessAvroMessage(msg []byte, transformConfig *config.MessageTransformConfig, transformedMessages chan<- []byte, failedMessages chan<- FailedMessage) error {
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
	transformedData, failedData, errors := transformAvroData(reader, transformConfig)
	if len(errors) > 0 {
		failedMessages <- FailedMessage{Data: failedData, Errors: errors}
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

func transformAvroData(reader *goavro.OCFReader, transformConfig *config.MessageTransformConfig) ([]map[string]interface{}, []map[string]interface{}, []error) {
	var transformedData []map[string]interface{}
	var failedData []map[string]interface{}
	var errors []error

	for reader.Scan() {
		// Decode the message using the reader
		v, err := reader.Read()
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to read Avro message: %v", err))
			continue
		}
		record, ok := v.(map[string]interface{})
		if !ok {
			errors = append(errors, fmt.Errorf("failed to decode Avro message: expected map[string]interface{}; received %T", v))
			failedData = append(failedData, record)
			continue
		}
		transformedRecord, err := applyRecordTransformations(record, transformConfig)
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to transform record: %v", err))
			failedData = append(failedData, record)
			continue
		}
		transformedData = append(transformedData, transformedRecord)
	}

	return transformedData, failedData, errors
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

func applyRecordTransformations(record map[string]interface{}, transformConfig *config.MessageTransformConfig) (map[string]interface{}, error) {
	if temperature, ok := record["temperature"].(float32); ok {
		// Convert temperature from Celsius to Fahrenheit
		record["temperature"] = temperature*9.0/5.0 + 32.0
	}
	// Add additional transformations here
	return record, nil
}
