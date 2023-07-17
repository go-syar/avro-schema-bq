package schema

import (
	"encoding/json"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestConvertAvroToBigQuery(t *testing.T) {
	// Test data: Avro schema in JSON format
	avroSchemaJSON := `
	{
		"type": "record",
		"name": "Person",
		"fields": [
			{"name": "Name", "type": "string"},
			{"name": "Age", "type": "int"},
			{"name": "Address", "type": "string"}
		]
	}`

	// Convert the JSON Avro schema to a byte slice
	avroSchemaContent := []byte(avroSchemaJSON)

	// Unmarshal the Avro schema
	var avroSchema map[string]interface{}
	err := json.Unmarshal(avroSchemaContent, &avroSchema)
	if err != nil {
		t.Fatalf("Error parsing Avro schema: %v", err)
	}

	// Call the ConvertAvroToBigQuery function
	bqFields, err := ConvertAvroToBigQuery(avroSchema)
	if err != nil {
		t.Fatalf("Error converting Avro to BigQuery schema: %v", err)
	}

	// Test the result
	expectedFields := bigquery.Schema{
		{Name: "Name", Type: "STRING"},
		{Name: "Age", Type: "INTEGER"},
		{Name: "Address", Type: "STRING"},
	}

	if len(bqFields) != len(expectedFields) {
		t.Fatalf("Expected %d fields, but got %d", len(expectedFields), len(bqFields))
	}

	for i, field := range bqFields {
		if field.Name != expectedFields[i].Name || field.Type != expectedFields[i].Type {
			t.Fatalf("Field %d: Expected (%s, %s), but got (%s, %s)", i, expectedFields[i].Name, expectedFields[i].Type, field.Name, field.Type)
		}
	}
}
