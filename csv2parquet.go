package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	isVerbose      bool
	delimiter      string
	isTabDelimited bool
	isHelp         bool
	nFields        int
	fieldNames     []string
	fieldTypes     []string
	fieldLayouts   []string
)

// Function to add an item (string) to a list (string) with ,\n\t as delimiter
func addItem(list string, item string) string {
	if list != "" {
		list += ",\n\t"
	}
	list += item
	return list
}

// Identify function to return a string as a string
func toString(x string) string {
	return x
}

// Return Unix time for a timestamp in string and a given time layout
func toDate(x string, layout string) int32 {
	t, err := time.Parse(layout, x)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error with date '%v', not following format '%v'\n", x, layout)
		return 0
	}
	return int32(t.Unix() / 60 / 60 / 24)
}

// Return Unix time in milliseconds for a timestamp in string and a given time layout
func toTimestamp(x string, layout string) int64 {
	t, err := time.Parse(layout, x)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error with timestamp '%v', not following format '%v'\n", x, layout)
		return 0
	}
	return t.Unix() * 1000
}

// Return an int64 from a string, ignoring any error (e.g. non-numbers are zero)
func toInt(x string) int64 {
	i, _ := strconv.ParseInt(x, 10, 64)
	return i
}

// Return a float64 from a string, ignoring any error (e.g. non-numbers are zero)
func toFloat(x string) float64 {
	f, _ := strconv.ParseFloat(x, 64)
	return f
}

// Return a time.Time from a set of year, month, day, time in string
func getTime(my_year, my_month, my_day, my_time string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", my_year+"-"+my_month+"-"+my_day+" "+my_time)
}

// If isVerbose flag is set, print a debug message
func Debug(format string, a ...interface{}) {
	if isVerbose {
		fmt.Printf(format+"\n", a...)
	}
}

// Print an error and exit program
func ErrorExit(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

// Assess a unkown data element and return
// * Parquet datatype
// * time layout
// * Type reflection
func assess(data string) (string, string, reflect.Type) {

	if _, err := strconv.Atoi(data); err == nil {
		return "INT64",
			"",
			reflect.TypeOf(int64(0))
	}

	if _, err := strconv.ParseFloat(data, 64); err == nil {
		return "DOUBLE",
			"",
			reflect.TypeOf(float64(0))
	}

	if _, err := time.Parse(time.RFC3339, data); err == nil {
		return "TIMESTAMP_MILLIS",
			time.RFC3339,
			reflect.TypeOf(int64(0))
	}

	if _, err := time.Parse("2006-01-02 15:04:05", data); err == nil {
		return "TIMESTAMP_MILLIS",
			"2006-01-02 15:04:05",
			reflect.TypeOf(int64(0))
	}

	if _, err := time.Parse("2006-01-02", data); err == nil {
		return "DATE",
			"2006-01-02",
			reflect.TypeOf(int32(0))
	}

	if _, err := time.Parse("2006/01/02", data); err == nil {
		return "DATE",
			"2006/01/02",
			reflect.TypeOf(int32(0))
	}

	return "BYTE_ARRAY",
		"",
		reflect.TypeOf(string(""))
}

// Open a CSV file and return a structure in Reflect
func detectSchema(filename string) []reflect.StructField {

	// Open file "filename"
	file, err := os.Open(filename)
	if err != nil {
		ErrorExit("Error: cannot open CSV file '%v': %v", filename, err)
	}

	// Prepare a scanner on the file
	scanner := bufio.NewScanner(file)

	// Read first line
	if scanner.Scan() == false {
		if err := scanner.Err(); err != nil {
			ErrorExit("Error: reading CSV file '%v': %v", filename, err)
		}
		ErrorExit("Error: file empty or too small")
	}
	line1 := scanner.Text()

	// Read 2nd line
	if scanner.Scan() == false {
		if err := scanner.Err(); err != nil {
			ErrorExit("Error: reading CSV file '%v': %v", filename, err)
		}
		ErrorExit("Error: file empty or too small")
	}
	line2 := scanner.Text()

	// Close file
	file.Close()

	// Extract field names from 1st line
	fieldNames = strings.Split(line1, delimiter)

	// Extract data as examples to auto-define the schema from the 2nd line
	data := strings.Split(line2, delimiter)

	// Set number of fields/columns
	nFields = len(fieldNames)

	// Reserve memory for schema structure
	structFields := make([]reflect.StructField, nFields, nFields)

	// Reserve memory for an array of field/column types
	fieldTypes = make([]string, nFields, nFields)

	// Reserve memory for an array of field/column time layout
	fieldLayouts = make([]string, nFields, nFields)

	fmt.Printf("Structure:\n")

	// Loop on all field/column data examples to define data types
	for i := 0; i < nFields; i++ {
		var fieldType reflect.Type

		// Assess parquet type, time layout, and Reflect type from 2nd row example
		fieldTypes[i],
			fieldLayouts[i],
			fieldType = assess(data[i])

		// Add Reflect strucure to schema
		structFields[i] = reflect.StructField{
			Name: strings.Title(fieldNames[i]),
			Type: fieldType,
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"name=%v, type=%v"`, fieldNames[i], fieldTypes[i])),
		}

		fmt.Printf("  %v: %v\n", fieldNames[i], fieldTypes[i])
	}

	return structFields
}

// Convert a line of data ([]string) into a Parquet reflection based on the field schema structure
func convertData(data []string, structFields []reflect.StructField) reflect.Value {
	dataType := reflect.StructOf(structFields)
	v := reflect.New(dataType).Elem()
	for i := 0; i < nFields; i++ {
		if fieldTypes[i] == "INT64" {
			v.Field(i).SetInt(toInt(data[i]))
		} else if fieldTypes[i] == "DOUBLE" {
			v.Field(i).SetFloat(toFloat(data[i]))
		} else if fieldTypes[i] == "FLOAT32" {
			v.Field(i).SetFloat(toFloat(data[i]))
		} else if fieldTypes[i] == "FLOAT64" {
			v.Field(i).SetFloat(toFloat(data[i]))
		} else if fieldTypes[i] == "BYTE_ARRAY" {
			v.Field(i).SetString(data[i])
		} else if fieldTypes[i] == "DATE" {
			v.Field(i).SetInt(int64(toDate(data[i], fieldLayouts[i])))
		} else if fieldTypes[i] == "TIMESTAMP_MILLIS" {
			v.Field(i).SetInt(toTimestamp(data[i], fieldLayouts[i]))
		} else {
			ErrorExit("Error, unkown type %v", fieldTypes[i])
		}
	}
	return v
}

// Read a CSV file, convert it to parquet based on a field schema structure
func readAndWrite(csv_filename string,
	parquet_filename string,
	structFields []reflect.StructField) {

	// Define Parquet File Writer
	Debug("Creating NewLocalFileWriter")
	fw, err := local.NewLocalFileWriter(parquet_filename)
	if err != nil {
		ErrorExit("Error: Can't create parquet file: %v", err)
	}
	defer fw.Close()

	// Define Parquet Writer pw on File Writer fw
	Debug("Creating NewParquetWriter:%v", structFields)
	dataType := reflect.StructOf(structFields)
	v := reflect.New(dataType).Elem()
	pw, err := writer.NewParquetWriter(fw, v.Addr().Interface(), 4)
	if err != nil {
		ErrorExit("Error: Can't create parquet writer: %v", err)
	}

	// Open CSV File
	Debug("Open CSV File")
	csv_file, err := os.Open(csv_filename)
	if err != nil {
		ErrorExit("Error: Can't open CSV file '%v': %v", csv_filename, err)
	}

	// Define file scanner
	scanner := bufio.NewScanner(csv_file)

	// Ignore first line (with headers)
	Debug("Ignoring first line")
	if scanner.Scan() == false {
		if err := scanner.Err(); err != nil {
			ErrorExit("Error: reading CSV file '%v': %v", csv_file, err)
		}
		ErrorExit("Error: file empty or too small")
	}

	// Loop throw each row of CSV file
	nRows := 0
	for scanner.Scan() {
		nRows++

		// Get line (string)
		line := scanner.Text()
		Debug("Read:%v", line)

		// Convert data to Reflect values
		v := convertData(strings.Split(line, delimiter), structFields)

		// Add data to parquet file
		Debug("Writing:%v", v)
		if err = pw.Write(v.Addr().Interface()); err != nil {
			ErrorExit("Error writing to parquet: %v", err)
		}
	}
	if err := scanner.Err(); err != nil {
		ErrorExit("Error: reading CSV file '%v': %v", csv_filename, err)
	}
	// Stop Parquet Writer pw
	if err = pw.WriteStop(); err != nil {
		ErrorExit("WriteStop error", err)
	}

	fmt.Printf("Parquet file %v written with %v rows and %v fields\n", parquet_filename, nRows, nFields)

}

func main() {

	// Parse command lines flag and arguments
	flag.BoolVar(&isVerbose, "v", false, "verbose mode")
	flag.BoolVar(&isTabDelimited, "t", false, "tab delimited")
	flag.BoolVar(&isHelp, "h", false, "help")
	flag.StringVar(&delimiter, "d", ",", "delimiter")
	flag.Parse()

	// Help
	if isHelp {
		fmt.Println(`Usage:
csv2parquet csv_file parquet_file`)
		os.Exit(0)
	}

	// Check that either tab or customer delimiter is set
	if isTabDelimited {
		if delimiter == "," {
			delimiter = "\t"
		} else {
			ErrorExit("Error: you can't use -t and -d at the same time")
		}
	}

	// Error if bad requests (with 1 or 3+ arguments in command line)
	if len(flag.Args()) != 2 {
		ErrorExit("Usage:\ncsv2parquet csv_file parquet_file")
	}

	// Get filenames from command line arguments
	csv_filename := flag.Arg(0)
	parquet_filename := flag.Arg(1)

	fmt.Printf(`CSV2PARQUET
CSV file:      %v
Parquet file:  %v
`, csv_filename, parquet_filename)

	// Detect schema with header on 1st row and one line of data on 2nd row
	structFields := detectSchema(csv_filename)

	// Read all CSV file and write to parquet file
	readAndWrite(csv_filename, parquet_filename, structFields)

}
