// Package schema provides functionality for converting AVRO Schema to BQ Schema
package schema

import (
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/bigquery"
)

// ConvertAvroToBigQuery converts an Avro schema represented as a map
// (avroSchema) to a BigQuery schema represented as a slice of
// bigquery.FieldSchema. It iterates through each field in the Avro
// schema, determines its data type, and creates a corresponding
// bigquery.FieldSchema with metadata like name, type, and description.
// The resulting BigQuery schema fields are returned as a slice.
// If any invalid field or type is encountered, an error is returned.

func checkPrecisionScale(fieldType map[string]interface{}) (int64, int64) {
	if fieldType["logicalType"].(string) == "decimal" {
		precisionValue, ok := fieldType["precision"].(float64)
		if !ok {
			return 0, 0
		}
		fmt.Println("precisionValue:", precisionValue)
		scale, ok := fieldType["scale"].(float64)
		if !ok {
			return int64(precisionValue), 0
		}
		fmt.Println("scale:", scale)
		return int64(precisionValue), int64(scale)
	}
	return 0, 0
}

func ConvertAvroToBigQuery(avroSchema map[string]interface{}) ([]*bigquery.FieldSchema, error) {
	var fields []*bigquery.FieldSchema

	// Extract the "fields" from the Avro schema.
	avroFields, ok := avroSchema["fields"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid Avro schema")
	}

	// Iterate through each field in the Avro schema.
	for _, avroField := range avroFields {
		// Assert the avroField to a map[string]interface{} to access its properties.
		avroFieldMap, ok := avroField.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid Avro schema field")
		}

		// Extract the optional "doc" field from the Avro field as its description.
		description, _ := avroFieldMap["doc"].(string)

		fmt.Println("default type:", reflect.TypeOf(avroFieldMap["default"]))
		defaultValue, _ := avroFieldMap["default"].(string)
		fmt.Println("defaultValue:", defaultValue)

		// Extract the required "name" field from the Avro field as the BigQuery field name.
		fieldName, ok := avroFieldMap["name"].(string)
		if !ok {
			return nil, fmt.Errorf("invalid Avro schema field name")
		}

		fmt.Println("fieldName: ", fieldName)

		fmt.Println("fieldTypeDataType: ", reflect.TypeOf(avroFieldMap["type"]))

		// Determine the type of the Avro field and convert it to the corresponding BigQuery type.
		switch avroFieldMap["type"].(type) {
		case []interface{}:
			// The Avro field has multiple types (e.g., union).
			fieldType, ok := avroFieldMap["type"].([]interface{})
			if !ok {
				return nil, fmt.Errorf("invalid Avro schema field type")
			}
			// Iterate through the types and convert each one to BigQuery field(s).
			for _, avroField := range fieldType {
				switch avroField.(type) {
				case map[string]interface{}:
					// The type is a nested record. Convert recursively.
					b := avroField
					repeated := false
					precision := int64(0)
					scale := int64(0)
					if b.(map[string]interface{})["type"].(string) == "bytes" {
						precision, scale = checkPrecisionScale(b.(map[string]interface{}))
					}

					if b.(map[string]interface{})["type"].(string) == "array" {
						repeated = true
					}

					bqFieldType, bqFieldSchema, err := convertAvroTypeToBigQuery(b.(map[string]interface{}))
					if err != nil {
						return nil, err
					}
					// Create the BigQuery field with the converted schema.
					field := &bigquery.FieldSchema{
						Name:                   fieldName,
						Type:                   bqFieldType,
						Schema:                 bqFieldSchema,
						Description:            description,
						Required:               false,
						Repeated:               repeated,
						Precision:              precision,
						Scale:                  scale,
						DefaultValueExpression: defaultValue,
					}
					fields = append(fields, field)

				case string:
					// The type is a primitive type.
					if avroField == "null" {
						fmt.Println("type null for []interface")
					} else {
						c := avroField
						bqFieldType, err := convertAvroStringTypeToBigQuery(c.(string))
						if err != nil {
							return nil, fmt.Errorf("invalid Avro schema field type")
						}
						fmt.Println("bqFieldType: ", bqFieldType)
						// Create the BigQuery field with the primitive type.
						field := &bigquery.FieldSchema{
							Name:                   fieldName,
							Type:                   bqFieldType,
							Description:            description,
							Required:               false,
							DefaultValueExpression: defaultValue,
						}
						fields = append(fields, field)
					}
				}
			}
		case string:
			// The Avro field has a single primitive type.
			fieldType, ok := avroFieldMap["type"].(string)
			if !ok {
				return nil, fmt.Errorf("invalid Avro schema field type")
			}
			var bqFieldSchema bigquery.Schema
			if fieldType == "record" {
				// The Avro type is a record, recursively convert the record's fields.
				fields, ok := avroFieldMap["fields"].([]interface{})
				if !ok {
					return nil, fmt.Errorf("invalid avro record fields")
				}

				// Recursively convert record fields using the ConvertAvroToBigQuery function.
				recordFields, err := ConvertAvroToBigQuery(map[string]interface{}{"fields": fields})
				if err != nil {
					bqFieldSchema = nil
				}

				// The record in Avro is mapped to a BigQuery RECORD type, with the schema of the record fields.
				bqFieldSchema = recordFields
			}
			// Convert the primitive type to BigQuery type.
			bqFieldType, err := convertAvroStringTypeToBigQuery(fieldType)
			if err != nil {
				return nil, fmt.Errorf("invalid Avro schema field type")
			}
			// Create the BigQuery field with the primitive type.
			field := &bigquery.FieldSchema{
				Name:                   fieldName,
				Type:                   bqFieldType,
				Schema:                 bqFieldSchema,
				Description:            description,
				Required:               true,
				DefaultValueExpression: defaultValue,
			}
			fields = append(fields, field)

		case map[string]interface{}:
			// The Avro field is a nested record.
			fieldType, ok := avroFieldMap["type"].(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("invalid Avro schema field type")
			}

			fmt.Println("avro test type: ", fieldType["type"].(string))

			repeated := false
			precision := int64(0)
			scale := int64(0)
			if fieldType["type"].(string) == "bytes" {
				precision, scale = checkPrecisionScale(fieldType)
			}
			if fieldType["type"].(string) == "array" {
				repeated = true
			}
			// Convert the nested type to BigQuery field(s).
			bqFieldType, bqFieldSchema, err := convertAvroTypeToBigQuery(fieldType)
			if err != nil {
				return nil, err
			}
			// Create the BigQuery field with the nested schema.
			field := &bigquery.FieldSchema{
				Name:                   fieldName,
				Type:                   bqFieldType,
				Schema:                 bqFieldSchema,
				Description:            description,
				Required:               true,
				Repeated:               repeated,
				Precision:              precision,
				Scale:                  scale,
				DefaultValueExpression: defaultValue,
			}
			fields = append(fields, field)

		default:
			// The Avro field type is not recognized as a valid type.
			// In this case, we treat it as a string type.
			field := &bigquery.FieldSchema{
				Name:                   fieldName,
				Type:                   bigquery.StringFieldType,
				Description:            description,
				DefaultValueExpression: defaultValue,
			}
			fields = append(fields, field)
		}
	}
	return fields, nil
}

// convertAvroStringTypeToBigQuery converts a given Avro data type (bqFieldType) to the corresponding BigQuery
// data type (bigquery.FieldType). It maps Avro data types to their equivalent BigQuery data types based on the
// switch cases. If a valid mapping is found, it returns the corresponding BigQuery data type and nil error.
// If the provided Avro data type is not recognized or mapped to a BigQuery data type, it defaults to the
// bigquery.StringFieldType and returns nil error.

func convertAvroStringTypeToBigQuery(bqFieldType string) (bigquery.FieldType, error) {
	switch bqFieldType {
	case "null":
		return bigquery.StringFieldType, nil
	case "boolean":
		return bigquery.BooleanFieldType, nil
	case "int":
		return bigquery.IntegerFieldType, nil
	case "long":
		return bigquery.IntegerFieldType, nil
	case "float":
		return bigquery.FloatFieldType, nil
	case "double":
		return bigquery.FloatFieldType, nil
	case "bytes":
		return bigquery.BytesFieldType, nil
	case "string":
		return bigquery.StringFieldType, nil
	case "enum":
		return bigquery.StringFieldType, nil
	case "fixed":
		// The Avro type is fixed, map to BigQuery BYTES type.
		return bigquery.BytesFieldType, nil
	case "record":
		// The Avro type is fixed, map to BigQuery BYTES type.
		return bigquery.RecordFieldType, nil
	default:
		return bigquery.StringFieldType, nil
	}

}

// convertAvroTypeToBigQuery converts an Avro type represented as a map (avroType) to the corresponding BigQuery
// data type (bigquery.FieldType) and schema (bigquery.Schema). It also handles nested types, such as arrays and records.
// The function uses a switch statement to map Avro types to their equivalent BigQuery types.
// If the Avro type is a simple primitive type, it returns the corresponding BigQuery type with a nil schema and error.
// If the Avro type is an array, it recursively converts the array's element type and returns a BigQuery RECORD type
// with the schema of the element type.
// If the Avro type is a record, it recursively converts the record's fields and returns a BigQuery RECORD type
// with the schema of the record fields.
// If the provided Avro type is not recognized or unsupported, it returns an error with a BigQuery RECORD type.

func convertAvroTypeToBigQuery(avroType map[string]interface{}) (bigquery.FieldType, bigquery.Schema, error) {
	typeName, ok := avroType["type"].(string)
	fmt.Println("typeName:", typeName)
	if !ok {
		return bigquery.RecordFieldType, nil, fmt.Errorf("invalid avro type")
	}

	switch typeName {
	case "null":
		// The Avro type is null, map to BigQuery STRING type (use STRING as a placeholder).
		return bigquery.StringFieldType, nil, nil
	case "fixed":
		// The Avro type is fixed, map to BigQuery BYTES type.
		return bigquery.BytesFieldType, nil, nil
	case "bytes":
		if avroType["logicalType"].(string) == "decimal" {
			precisionValue, scale := checkPrecisionScale(avroType)
			if 0 <= (precisionValue-scale) && (precisionValue-scale) <= 29 && scale <= 9 && precisionValue <= 38 {
				// The Avro type is bytes, logicalType is decimal and precision > 38 map to BigQuery BigNumeric type.
				return bigquery.NumericFieldType, nil, nil
			} else if 0 <= (precisionValue-scale) && (precisionValue-scale) <= 38 && scale <= 38 && precisionValue <= 76 {
				return bigquery.BigNumericFieldType, nil, nil
			} else {
				// The Avro type is bytes, logicalType is decimal map to BigQuery Numeric type.
				return bigquery.NumericFieldType, nil, fmt.Errorf("precision and scale are out of bounds")
			}
		}
		// The Avro type is bytes, map to BigQuery BYTES type.
		return bigquery.BytesFieldType, nil, nil
	case "int":
		switch avroType["logicalType"].(string) {
		case "date":
			// The Avro type is int, logicalType is date map to BigQuery Date type.
			return bigquery.DateFieldType, nil, nil
		case "time-millis":
			// The Avro type is int, logicalType is time-millis map to BigQuery Time type.
			return bigquery.TimeFieldType, nil, nil
		}
		// The Avro type is int, map to BigQuery INTEGER type.
		return bigquery.IntegerFieldType, nil, nil
	case "long":
		switch avroType["logicalType"].(string) {
		case "time-micros":
			// The Avro type is int, logicalType is date map to BigQuery Date type.
			return bigquery.TimeFieldType, nil, nil
		case "timestamp-millis":
			// The Avro type is int, logicalType is time-millis map to BigQuery Time type.
			return bigquery.TimestampFieldType, nil, nil
		case "timestamp-micros":
			// The Avro type is int, logicalType is date map to BigQuery Date type.
			return bigquery.TimestampFieldType, nil, nil
		case "local-timestamp-millis":
			// The Avro type is int, logicalType is time-millis map to BigQuery Time type.
			return bigquery.DateTimeFieldType, nil, nil
		case "local-timestamp-micros":
			// The Avro type is int, logicalType is time-millis map to BigQuery Time type.
			return bigquery.DateTimeFieldType, nil, nil
		}
		// The Avro type is long, map to BigQuery INTEGER type.
		return bigquery.IntegerFieldType, nil, nil
	case "boolean":
		// The Avro type is boolean, map to BigQuery BOOLEAN type.
		return bigquery.BooleanFieldType, nil, nil
	case "float":
		// The Avro type is float, map to BigQuery FLOAT type.
		return bigquery.FloatFieldType, nil, nil
	case "double":
		// The Avro type is double, map to BigQuery FLOAT type.
		return bigquery.FloatFieldType, nil, nil
	case "string":
		// The Avro type is string, map to BigQuery STRING type.
		if strings.ToLower(avroType["sqlType"].(string)) == "json" {
			return bigquery.JSONFieldType, nil, nil
		}
		return bigquery.StringFieldType, nil, nil
	case "enum":
		// The Avro type is an enum, map to BigQuery STRING type (use STRING as a placeholder for enum).
		return bigquery.StringFieldType, nil, nil
	case "array":
		// The Avro type is an array, recursively convert the array's element type.
		switch avroType["items"].(type) {
		case string:
			items, ok := avroType["items"].(string)
			if !ok {
				return bigquery.StringFieldType, nil, fmt.Errorf("invalid avro array items")
			}
			itemsbqtype, err := convertAvroStringTypeToBigQuery(items)
			if err != nil {
				return bigquery.RecordFieldType, nil, err
			}
			elementSchema := bigquery.Schema{{Name: avroType["name"].(string), Type: itemsbqtype}}
			// The array in Avro is mapped to a BigQuery RECORD type, with the schema of the element type.
			return bigquery.RecordFieldType, elementSchema, nil
		case map[string]interface{}:
			items, ok := avroType["items"].(map[string]interface{})
			if !ok {
				return bigquery.StringFieldType, nil, fmt.Errorf("invalid avro array items")
			}
			elementType, elementSchema, err := convertAvroTypeToBigQuery(items)
			if err != nil {
				return bigquery.RecordFieldType, nil, err
			}
			fmt.Println("elementType: ", elementType)

			// The array in Avro is mapped to a BigQuery RECORD type, with the schema of the element type.
			return bigquery.RecordFieldType, elementSchema, nil
		}
	case "record":
		// The Avro type is a record, recursively convert the record's fields.
		fields, ok := avroType["fields"].([]interface{})
		if !ok {
			return bigquery.RecordFieldType, nil, fmt.Errorf("invalid avro record fields")
		}

		// Recursively convert record fields using the ConvertAvroToBigQuery function.
		recordFields, err := ConvertAvroToBigQuery(map[string]interface{}{"fields": fields})
		if err != nil {
			return bigquery.RecordFieldType, nil, err
		}

		// The record in Avro is mapped to a BigQuery RECORD type, with the schema of the record fields.
		return bigquery.RecordFieldType, recordFields, nil
	default:
		// The Avro type is not recognized or unsupported, return an error with a BigQuery RECORD type.
		return bigquery.RecordFieldType, nil, fmt.Errorf("unsupported avro type: %s", typeName)
	}
	return bigquery.StringFieldType, nil, fmt.Errorf("unsupported avro type: %s", typeName)
}
