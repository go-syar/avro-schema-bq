// Package main provides functionality for displaying the use of schema package to convert AVRO Schema to BQ Schema

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/go-syar/avro-schema-bq/schema"
)

// main is the entry point of the program.
func main() {
	// schemaFilePath contains the path to the Avro schema file.
	schemaFilePath := "schema/test_data/testfile.avsc"

	// avroSchemaContent stores the content of the Avro schema file read from disk.
	avroSchemaContent, err := ioutil.ReadFile(schemaFilePath)
	if err != nil {
		fmt.Println("Error reading Avro schema file:", err)
		return
	}
	// avroSchema is a map that holds the parsed Avro schema data.
	var avroSchema map[string]interface{}
	err = json.Unmarshal(avroSchemaContent, &avroSchema)
	if err != nil {
		fmt.Println("Error parsing Avro schema:", err)
		return
	}

	// Print the parsed Avro schema.
	fmt.Println("avroschema: ", avroSchema)

	// ConvertAvroToBigQuery converts the Avro schema to BigQuery schema.
	bqFields, err := schema.ConvertAvroToBigQuery(avroSchema)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Print the BigQuery schema.
	fmt.Println("BigQuery Schema:")
	for _, field := range bqFields {
		fmt.Println(field.Name, field.Type)
		if field.Type == "RECORD" && field.Schema != nil {
			for _, subField := range field.Schema {
				fmt.Println(subField.Name, subField.Type)
			}
		}
	}

	// Convert the BigQuery schema to JSON
	jsonData, err := json.MarshalIndent(bqFields, "", "    ")
	if err != nil {
		fmt.Println("Error marshaling BigQuery schema to JSON:", err)
		return
	}

	// Write the JSON data to a file
	err = ioutil.WriteFile("schema/test_data/bq_schema.json", jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing JSON data to file:", err)
		return
	}

}
