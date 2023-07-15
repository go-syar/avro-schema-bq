package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type schemaJsonResponse struct {
	Type    string          `json:"type"`
	Data    bigquery.Schema `json:"data"`
	Message string          `json:"message"`
}

func main() {
	projectID := "csw-data-gateway-nonprod"
	datasetID := "data_gateway_test"
	tableID := "tablename"
	schemaFilePath := "schemafilepath"
	serviceAccount := "csw-data-gateway-nonprod-api@csw-data-gateway-nonprod.iam.gserviceaccount.com.Json"
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	//var response = schemaJsonResponse{}

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

	bqFields, err := schema.convertAvroToBigQuery(avroSchema)
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
	fmt.Println("BigQuery Schema: ", bqFields)
	metadata := &bigquery.TableMetadata{
		Schema: bqFields,
		Labels: map[string]string{
			"application":         "data_gateway",
			"table_creation_type": "kafka",
		},
		ExpirationTime: time.Now().AddDate(1, 0, 0),
	}
	fmt.Println("BigQuery metadata: ", metadata)

	tableRef := client.Dataset(datasetID).Table(tableID)
	if err := tableRef.Create(ctx, metadata); err != nil {
		return
	}

	//response = schemaJsonResponse{Type: "success", Data: bqFields, Message: "Successfully Created BQ Table"}
	//json.NewEncoder(w).Encode(response)

}
