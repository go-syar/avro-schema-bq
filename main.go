package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/go-syar/avro-schema-bq/schema"
)

// type schemaJsonResponse struct {
// 	Type    string          `json:"type"`
// 	Data    bigquery.Schema `json:"data"`
// 	Message string          `json:"message"`
// }

func main() {

	schemaFilePath := "schema/test_data/testfile.avsc"

	avroSchemaContent, err := ioutil.ReadFile(schemaFilePath)
	if err != nil {
		fmt.Println("Error reading Avro schema file:", err)
		return
	}

	var avroSchema map[string]interface{}
	err = json.Unmarshal(avroSchemaContent, &avroSchema)
	if err != nil {
		fmt.Println("Error parsing Avro schema:", err)
		return
	}

	fmt.Println("avroschema: ", avroSchema)

	bqFields, err := schema.ConvertAvroToBigQuery(avroSchema)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

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
