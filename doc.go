// Copyright © 2023 Srijanya Yarram. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Convert Apache Avro schema to BigQuery Table Schema.
This Package supports Avro schema with Record/Array data types as well.

# Import Package

-- go install github.com/go-syar/avro-schema-bq@latest

# Create BQ Table with Avro Schema (avsc)

Required arguments projectID, datasetID, tableID, serviceAccount, schemaFilePath

-- table.CreateBQTableWithSA(projectID string, datasetID string, tableID string, serviceAccount string, schemaFilePath string) error
// service account := "service-account.json"

# Avro Schema (avsc) to BQ Schema (json)

schema.ConvertAvroToBigQuery(avroSchema map[string]interface{}) ([]*bigquery.FieldSchema, error)
*/

package main // import "golang.org/x/tools/cmd/godoc"
