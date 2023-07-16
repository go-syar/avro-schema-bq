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

	avroSchemaContent, err := ioutil.ReadFile(schemaFilePath)
	if err != nil {
		fmt.Println("Error reading Avro schema file:", err)
		return err
	}

	var avroSchema map[string]interface{}
	err = json.Unmarshal(avroSchemaContent, &avroSchema)
	if err != nil {
		fmt.Println("Error parsing Avro schema:", err)
		return err
	}

	bqFields, err := schema.ConvertAvroToBigQuery(avroSchema)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}

	metadata := &bigquery.TableMetadata{
		Schema: bqFields,
	}

	tableRef := client.Dataset(datasetID).Table(tableID)
	if err := tableRef.Create(ctx, metadata); err != nil {
		return err
	}

	return nil

}
