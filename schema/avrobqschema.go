package schema

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/bigquery"
)

func ConvertAvroToBigQuery(avroSchema map[string]interface{}) ([]*bigquery.FieldSchema, error) {
	var fields []*bigquery.FieldSchema

	avroFields, ok := avroSchema["fields"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid Avro schema")
	}

	fmt.Println("avroFields: ", avroFields)

	for _, avroField := range avroFields {
		avroFieldMap, ok := avroField.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid Avro schema field")
		}
		fmt.Println("avroFieldMap:", avroFieldMap)

		description, _ := avroFieldMap["doc"].(string)
		fmt.Println("doc: ", description)

		fieldName, ok := avroFieldMap["name"].(string)
		if !ok {
			return nil, fmt.Errorf("invalid Avro schema field name")
		}

		fmt.Println("fieldName: ", fieldName)

		fmt.Println("fieldTypeDataType: ", reflect.TypeOf(avroFieldMap["type"]))

		switch avroFieldMap["type"].(type) {
		case []interface{}:
			fieldType, ok := avroFieldMap["type"].([]interface{})
			fmt.Println("fieldType: ", fieldType)
			if !ok {
				return nil, fmt.Errorf("invalid Avro schema field type")
			}
			for _, avroField := range fieldType {
				switch avroField.(type) {
				case map[string]interface{}:
					b := avroField
					bqFieldType, bqFieldSchema, err := convertAvroTypeToBigQuery(b.(map[string]interface{}))
					if err != nil {
						return nil, err
					}
					fmt.Println("bqFieldType: ", bqFieldType)
					field := &bigquery.FieldSchema{
						Name:        fieldName,
						Type:        bqFieldType,
						Schema:      bqFieldSchema,
						Description: description,
					}
					fields = append(fields, field)
					for _, field := range fields {
						fmt.Println("fields interface map: ", field)
						fmt.Println()
					}

				case string:
					if avroField == "null" {
						fmt.Println("type null for [linterface")
					} else {
						c := avroField
						bqFieldType, err := convertAvroStringTypeToBigQuery(c.(string))
						if err != nil {
							return nil, fmt.Errorf("invalid Avro schema field type")
						}
						fmt.Println("bqFieldType: ", bqFieldType)
						field := &bigquery.FieldSchema{
							Name:        fieldName,
							Type:        bqFieldType,
							Description: description,
						}
						fields = append(fields, field)
						for _, field := range fields {
							fmt.Println("fields interface string: ", field)
							fmt.Println()
						}
					}
				}
			}
		case string:
			fieldType, ok := avroFieldMap["type"].(string)
			fmt.Println("fieldType: ", fieldType)
			if !ok {
				return nil, fmt.Errorf("invalid Avro schema field type")
			}
			bqFieldType, err := convertAvroStringTypeToBigQuery(fieldType)
			if err != nil {
				return nil, fmt.Errorf("invalid Avro schema field type")
			}
			fmt.Println("bqFieldType: ", bqFieldType)
			field := &bigquery.FieldSchema{
				Name:        fieldName,
				Type:        bqFieldType,
				Description: description,
			}
			fields = append(fields, field)

			for _, field = range fields {
				fmt.Println("fields string: ", field)
				fmt.Println()
			}
		case map[string]interface{}:
			fieldType, ok := avroFieldMap["type"].(map[string]interface{})
			fmt.Println("fieldType: ", fieldType)
			if !ok {
				return nil, fmt.Errorf("invalid Avro schema field type")
			}
			bqFieldType, bqFieldSchema, err := convertAvroTypeToBigQuery(fieldType)
			if err != nil {
				return nil, err
			}
			fmt.Println("bqFieldType: ", bqFieldType)
			fmt.Println("bqFieldSchema: ", bqFieldSchema)
			field := &bigquery.FieldSchema{
				Name:        fieldName,
				Type:        bqFieldType,
				Schema:      bqFieldSchema,
				Description: description,
			}
			fields = append(fields, field)
			for _, field := range fields {
				fmt.Println("fields map: ", field)
				fmt.Println()
			}

		default:
			field := &bigquery.FieldSchema{
				Name:        fieldName,
				Type:        bigquery.StringFieldType,
				Description: description,
			}
			fields = append(fields, field)
		}
	}
	for _, field := range fields {
		fmt.Println("fields final: ", field)
		fmt.Println()
	}
	return fields, nil
}

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
	case "timestamp":
		return bigquery.TimestampFieldType, nil
	case "date":
		return bigquery.DateFieldType, nil
	case "time":
		return bigquery.TimeFieldType, nil
	case "datetime":
		return bigquery.DateTimeFieldType, nil
	}
	return bigquery.StringFieldType, nil

}

func convertAvroTypeToBigQuery(avroType map[string]interface{}) (bigquery.FieldType, bigquery.Schema, error) {
	typeName, ok := avroType["type"].(string)
	fmt.Println("typeName:", typeName)
	if !ok {
		return bigquery.RecordFieldType, nil, fmt.Errorf("Invalid Avro type")
	}

	switch typeName {
	case "null":
		return bigquery.StringFieldType, nil, nil
	case "boolean":
		return bigquery.BooleanFieldType, nil, nil
	case "int":
		return bigquery.IntegerFieldType, nil, nil
	case "long":
		return bigquery.IntegerFieldType, nil, nil
	case "float":
		return bigquery.FloatFieldType, nil, nil
	case "double":
		return bigquery.FloatFieldType, nil, nil
	case "bytes":
		return bigquery.BytesFieldType, nil, nil
	case "string":
		return bigquery.StringFieldType, nil, nil
	case "enum":
		return bigquery.StringFieldType, nil, nil
	case "timestamp":
		return bigquery.TimestampFieldType, nil, nil
	case "date":
		return bigquery.DateFieldType, nil, nil
	case "time":
		return bigquery.TimeFieldType, nil, nil
	case "datetime":
		return bigquery.DateTimeFieldType, nil, nil
	case "array":
		items, ok := avroType["items"].(map[string]interface{})
		if !ok {
			return bigquery.RecordFieldType, nil, fmt.Errorf("Invalid Avro array items")
		}
		elementType, elementSchema, err := convertAvroTypeToBigQuery(items)
		if err != nil {
			return bigquery.RecordFieldType, nil, err
		}
		fmt.Println("elementType: ", elementType)
		fmt.Println("elementSchema:", elementSchema)

		return bigquery.RecordFieldType, elementSchema, nil
	case "record":
		fields, ok := avroType["fields"].([]interface{})
		if !ok {
			return bigquery.RecordFieldType, nil, fmt.Errorf("Invalid Avro record fields")
		}

		// Recursively convert record fields
		fmt.Println("map[string]interface{}fields: ", (map[string]interface{}{"fields": fields}))
		recordFields, err := ConvertAvroToBigQuery(map[string]interface{}{"fields": fields})
		if err != nil {
			return bigquery.RecordFieldType, nil, err
		}
		fmt.Println("recordFields: ", recordFields)

		return bigquery.RecordFieldType, recordFields, nil
	default:
		return bigquery.RecordFieldType, nil, fmt.Errorf("unsupported Avro type: %s", typeName)
	}
}
