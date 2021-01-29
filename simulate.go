package main

import (
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// String of characters to pick from when creating randomString()
const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// Return a random string
func randomString() string {
	text := ""
	for i := 0; i < 3+rand.Intn(12); i++ {
		text += string(characters[rand.Intn(len(characters))])
	}
	return text
}

// Return a random unix time between Jan 1st 2021 and Jan 1st 2023
func randomTime() time.Time {
	return time.Unix(int64(1609459200+rand.Intn(2*365*24*60*60)), 0)
}

// Return a random date between Jan 1st 2021 and Jan 1st 2023
func toDate(x time.Time) int64 {
	return x.Unix() / 60 / 60 / 24
}

// Convert a Go timestamp into Parquet timestamp (Unix Time in milliseconds)
func toTimestamp(x time.Time) int64 {
	return x.Unix() * 1000
}

// Main program
func main() {

	// Throw an error if there is less than 4 command line
	if len(os.Args) < 4 {
		fmt.Printf("Usage:\ncreate file.parquet 100 X:INT32 Y:FLOAT32\n")
		os.Exit(1)
	}

	// 1st parameter: Parquet filename to create
	filename := os.Args[1]

	// 2nd parameter: Number of rows to simulate
	nRows, err := strconv.Atoi(os.Args[2])	
	if err != nil {
		fmt.Printf("Error: Invalid first parameter %v: %v\n", os.Args[2], err)
		os.Exit(1)
	}

	// 3+th parameter: Parquet fields/columns name and data type
	nFields := len(os.Args) - 3

	// Prepare slice/array with the reflect's field structure
	structFields := make([]reflect.StructField, nFields, nFields)

	// Prepare slice/array with the name/string of the field's data type
	fieldTypes := make([]string, nFields, nFields)

	for i := 0; i < nFields; i++ {
		// Split ith field (e.g. "X:INT32" -> ["X", "INT32"])
		elem := strings.Split(os.Args[3+i], ":")
		if len(elem) != 2 {
			fmt.Printf("Error: Invalid parameter #%v: %v\n", 3+i, os.Args[3+i])
			os.Exit(1)
		}

		// Create reflection of field type:
		var fieldType reflect.Type
		var fieldTag string
		switch strings.ToUpper(elem[1]) {
		case "INT", "INT32":
			fieldTypes[i] = "INT32"
			fieldType = reflect.TypeOf(int32(0))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=INT32"`, strings.ToLower(elem[0]))
			break
		case "FLOAT", "FLOAT32":
			fieldTypes[i] = "FLOAT32"
			fieldType = reflect.TypeOf(float32(0))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=FLOAT"`, strings.ToLower(elem[0]))
			break
		case "DOUBLE", "FLOAT64":
			fieldTypes[i] = "FLOAT64"
			fieldType = reflect.TypeOf(float64(0))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=DOUBLE"`, strings.ToLower(elem[0]))
			break
		case "VARCHAR", "UTF", "UTF8":
			fieldTypes[i] = "UTF8"
			fieldType = reflect.TypeOf(string(""))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=UTF8, encoding=PLAIN_DICTIONARY"`, strings.ToLower(elem[0]))
			break
		case "DATE":
			fieldTypes[i] = "DATE"
			fieldType = reflect.TypeOf(int32(0))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=DATE"`, strings.ToLower(elem[0]))
			break
		case "TIMESTAMP":
			fieldTypes[i] = "TIMESTAMP"
			fieldType = reflect.TypeOf(int64(0))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=TIMESTAMP_MILLIS"`, strings.ToLower(elem[0]))
			break
		default:
			fmt.Printf("Error: Invalid type for field %v: %v\n", 3+i, elem[1])
			os.Exit(1)
		}
		// Add new field to slice
		structFields[i] = reflect.StructField{
			Name: strings.ToUpper(elem[0]),
			Type: fieldType,
			Tag:  reflect.StructTag(fieldTag),
		}
	}	
	dataType := reflect.StructOf(structFields)

	// Create a file writer on the new parquet file
	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	// Defer closure of file writer
	defer func() {
		fw.Close()
	}()

	// Create a new Parquet writer on new file
	v := reflect.New(dataType).Elem()
	pw, err := writer.NewParquetWriter(fw, v.Addr().Interface(), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	// Define Row Group Size to 128M
	pw.RowGroupSize = 128 * 1024 * 1024 
	
	// Define Compression Type to SNAPPY
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Simulate nRows rows
	for i := 0; i < nRows; i++ {

		// Create new reflect slice/array to store the row with the rights data types
		v := reflect.New(dataType).Elem()

		// Simulate nFields columns
		for i := 0; i < nFields; i++ {

			// Enter random value into new variable
			if fieldTypes[i] == "INT32" {
				v.Field(i).SetInt(rand.Int63())
			} else if fieldTypes[i] == "FLOAT32" {
				v.Field(i).SetFloat(rand.NormFloat64())
			} else if fieldTypes[i] == "FLOAT64" {
				v.Field(i).SetFloat(rand.NormFloat64())
			} else if fieldTypes[i] == "UTF8" {
				v.Field(i).SetString(randomString())
			} else if fieldTypes[i] == "DATE" {
				v.Field(i).SetInt(toDate(randomTime()))
			} else if fieldTypes[i] == "TIMESTAMP" {
				v.Field(i).SetInt(toTimestamp(randomTime()))
			} else {
				fmt.Printf("Internal Error, unkown type %v\n", fieldTypes[i])
				return
			}
		}

		// Write row to parquet file
		if err = pw.Write(v.Addr().Interface()); err != nil {
			fmt.Println("Write error", err)
		}

	}

	// Close parquet file write
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}

	fmt.Printf("Parquet file %v written with %v rows and %v fields\n", filename, nRows, nFields)

}
