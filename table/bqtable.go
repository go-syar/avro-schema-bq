// Package table provides functionality for converting empty BQ Table with provided AVRO Schema
package table

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/go-syar/avro-schema-bq/schema"
	"google.golang.org/api/option"
)

func CreateBQTableWithSA(projectID, datasetID, tableID, serviceAccount, schemaFilePath string) error {
	// service account := "service-account.json"
	if projectID == "" || datasetID == "" || tableID == "" || serviceAccount == "" || schemaFilePath == "" {
		return fmt.Errorf("missing one of the required parameters")
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Read the contents of the Avro schema file from the specified path (schemaFilePath).
	avroSchemaContent, err := ioutil.ReadFile(schemaFilePath)
	if err != nil {
		fmt.Println("Error reading Avro schema file:", err)
		return err
	}

	var avroSchema map[string]interface{}
	// Unmarshal the Avro schema content into a map structure (avroSchema map[string]interface{}).
	err = json.Unmarshal(avroSchemaContent, &avroSchema)
	if err != nil {
		fmt.Println("Error parsing Avro schema:", err)
		return err
	}

	// Convert the Avro schema (avroSchema map[string]interface{}) to BigQuery schema format (bqFields []*bigquery.FieldSchema).
	bqFields, err := schema.ConvertAvroToBigQuery(avroSchema)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}

	// Create BigQuery table metadata (metadata) with the converted schema (bqFields bigquery.Schema).
	metadata := &bigquery.TableMetadata{
		Schema: bqFields,
	}

	// Create a reference to the BigQuery table using the specified dataset (datasetID) and table ID (tableID).
	tableRef := client.Dataset(datasetID).Table(tableID)
	// Create the BigQuery table using the provided table metadata (metadata).
	if err := tableRef.Create(ctx, metadata); err != nil {
		return err
	}

	return nil

}
