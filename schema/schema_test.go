package schema

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestConvertAvroToBigQuery(t *testing.T) {
	// Test data: Avro schema in JSON format
	t.Run("avro schema matches with converted big query schema", func(t *testing.T) {
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
	})
}

func TestConvertAvroToBigQueryFile(t *testing.T) {
	schemaFilePath := "test_data/testfile.avsc"

	// Read the contents of the Avro schema file from the specified path (schemaFilePath).
	avroSchemaContent, err := ioutil.ReadFile(schemaFilePath)
	if err != nil {
		t.Fatalf("Error reading Avro schema file: %v", err)
	}

	var avroSchema map[string]interface{}
	// Unmarshal the Avro schema content into a map structure (avroSchema map[string]interface{}).
	err = json.Unmarshal(avroSchemaContent, &avroSchema)
	if err != nil {
		t.Fatalf("Error parsing Avro schema: %v", err)
	}

	// Convert the Avro schema (avroSchema map[string]interface{}) to BigQuery schema format (bqFields []*bigquery.FieldSchema).
	bqFields, err := ConvertAvroToBigQuery(avroSchema)
	if err != nil {
		t.Fatalf("Error converting Avro schema: %v", err)
	}

	jsonData, err := json.MarshalIndent(bqFields, "", "    ")
	if err != nil {
		t.Fatalf("Error marshaling BigQuery schema to JSON: %v", err)
	}

	err = ioutil.WriteFile("test_data/bq_schema_multipletypes.json", jsonData, 0644)
	if err != nil {
		t.Fatalf("Error writing JSON data to file: %v", err)
	}
}
