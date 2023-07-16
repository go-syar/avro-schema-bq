# avro-schema-bq 
Convert [Apache Avro](https://avro.apache.org/docs/1.11.1/specification/) schema (it supports schemas with array/record types) to [BigQuery Table Schema](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema).

```sh
go install github.com/go-syar/avro-schema-bq@latest
```

## Usage

```sh
avro-bq-schema schema.avsc > bq.json
```

### Create BQ Table with Avro Schema (avsc)

Create BQ Table with Avro schema by providing variables projectID, datasetID, tableID, serviceAccount, schemaFilePath  
```sh
table.CreateBQTableWithSA(projectID string, datasetID string, tableID string, serviceAccount string, schemaFilePath string) error
// service account := "service-account.json"
```

### Avro Schema (avsc) to BQ Schema (json)

```sh
schema.ConvertAvroToBigQuery(avroSchema map[string]interface{}) ([]*bigquery.FieldSchema, error)
```

#### Convert .avsc file to map[string]interface{}

```sh
	schemaFilePath := $ your-(.avsc)file-path

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
```

#### Convert the BigQuery schema to JSON

```sh
	jsonData, err := json.MarshalIndent(bqFields, "", "    ")
	if err != nil {
		fmt.Println("Error marshaling BigQuery schema to JSON:", err)
		return
	}
```

#### Write the JSON BigQuery schema output to a file

```sh
	err = ioutil.WriteFile("schema/test_data/bq_schema.json", jsonData, 0644)
	if err != nil {
		fmt.Println("Error writing JSON data to file:", err)
		return
	}
```
