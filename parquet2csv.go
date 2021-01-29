package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/sizetool"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type FieldType struct {
	Tag    string      `json:"Tag,omitempty"`
	Fields []FieldType `json:"Fields,omitempty"`
}

var (
	doCreateTable           bool
	isVerbose               bool
	databasename, tablename string
	Host, User, Password    string
	dataType                reflect.Type
	folder                  string
	timeNano                string
	nFields                 int
	fieldChange             []string
	fcsv                    *os.File
)

func addItem(list string, item string) string {
	if list != "" {
		list += ",\n\t"
	}
	list += item
	return list
}

func Debug(format string, a ...interface{}) {
	if isVerbose {
		fmt.Printf(format+"\n", a...)
	}
}

func ErrorExit(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

func checkFieldName(field string) string {

	f := strings.ToUpper(field)

	switch f {
	case "TIME":
		f = "TIME_FIELD"
		break
	case "DATE":
		f = "DATE_FIELD"
		break
	}

	return f
}

func getString(v interface{}, change string) string {
	var x = fmt.Sprintf("%v", v)
	if change == "TIMESTAMP" {
		unixtime, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			ErrorExit("Error with value %v: %v", x, err)
		}
		return time.Unix(unixtime/1000, 0).Format("2006-01-02 15:04:05")
	}
	if change == "DATE" {
		days, err := strconv.Atoi(x)
		if err != nil {
			ErrorExit("Error with value %v: %v", x, err)
		}
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days).Format("2006-01-02")
	}

	return x
}

func CreateSchemaRead(filename string, csv_filename string) error {

	/**************************************************************
	       Create temp folder and output files
	**************************************************************/
	fr, err := local.NewLocalFileReader(filename)
	if err != nil {
		ErrorExit("Error, can't open file ", filename)
	}

	pcr, err := reader.NewParquetColumnReader(fr, 1)
	if err != nil {
		ErrorExit("Error, can't create parquet reader ", err)
	}

	Debug("Rows: %v", pcr.GetNumRows())
	Debug("File size (uncompressed): %v", sizetool.GetParquetFileSize(filename, pcr, true, true))
	Debug("File size (compressed): %v", sizetool.GetParquetFileSize(filename, pcr, true, false))

	tree := schematool.CreateSchemaTree(pcr.SchemaHandler.SchemaElements)
	var t FieldType
	json.Unmarshal([]byte(tree.OutputJsonSchema()), &t)

	nFields = len(t.Fields)
	structFields := make([]reflect.StructField, nFields, nFields)
	fieldTypes := make([]string, nFields, nFields)
	fieldChange = make([]string, nFields, nFields)

	Debug("Fields: %v", nFields)

	/**************************************************************
	       Assess schema dynamically
	**************************************************************/

	Debug("Structure:")

	fields_list := ""
	fields_list2 := ""

	for i, field := range tree.Root.Children {
		field_name := field.SE.GetName()
		field_type, field_type2 := schematool.ParquetTypeToParquetTypeStr(field.SE.Type, field.SE.ConvertedType)
		Debug("\t%v\t%v\t%v\n", field_name, field_type, field_type2)
		var fieldType reflect.Type
		var fieldTag string
		fieldChange[i] = ""
		fmt.Printf("field_type=%v field_type2=%v\n",field_type,field_type2)

		switch strings.ToUpper(field_type2) {
		case "INT", "INT32":
			fieldTypes[i] = "INT32"
			fieldType = reflect.TypeOf(int32(0))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=INT32"`, strings.ToLower(field_name))
			break
		case "INT64":
			fieldTypes[i] = "INT64"
			fieldType = reflect.TypeOf(int64(0))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=INT64"`, strings.ToLower(field_name))
			break
		case "FLOAT", "FLOAT32":
			fieldTypes[i] = "FLOAT32"
			fieldType = reflect.TypeOf(float32(0))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=FLOAT"`, strings.ToLower(field_name))
			break
		case "DOUBLE", "FLOAT64":
			fieldTypes[i] = "FLOAT64"
			fieldType = reflect.TypeOf(float64(0))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=DOUBLE"`, strings.ToLower(field_name))
			break
		case "VARCHAR", "UTF", "UTF8", "BYTE_ARRAY":
			fieldTypes[i] = "BYTE_ARRAY"
			fieldType = reflect.TypeOf(string(""))
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`, strings.ToLower(field_name))
			break
		case "DATE":
			fieldTypes[i] = "DATE"
			fieldType = reflect.TypeOf(int32(0))
			fieldChange[i] = "DATE"
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=DATE"`, strings.ToLower(field_name))
			break
		case "TIMESTAMP", "TIMESTAMP_MILLIS":
			fieldTypes[i] = "TIMESTAMP"
			fieldType = reflect.TypeOf(int64(0))
			fieldChange[i] = "TIMESTAMP"
			fieldTag = fmt.Sprintf(`parquet:"name=%v, type=TIMESTAMP_MILLIS"`, strings.ToLower(field_name))
			break
		default:
			switch strings.ToUpper(field_type) {
			case "INT", "INT32":
				fieldTypes[i] = "INT32"
				fieldType = reflect.TypeOf(int32(0))
				fieldTag = fmt.Sprintf(`parquet:"name=%v, type=INT32"`, strings.ToLower(field_name))
				break
			case "INT64":
				fieldTypes[i] = "INT64"
				fieldType = reflect.TypeOf(int64(0))
				fieldTag = fmt.Sprintf(`parquet:"name=%v, type=INT64"`, strings.ToLower(field_name))
				break
			case "DOUBLE", "FLOAT64":
				fieldTypes[i] = "FLOAT64"
				fieldType = reflect.TypeOf(float64(0))
				fieldTag = fmt.Sprintf(`parquet:"name=%v, type=DOUBLE"`, strings.ToLower(field_name))
				break
			case "FLOAT", "FLOAT32":
				fieldTypes[i] = "FLOAT32"
				fieldType = reflect.TypeOf(float32(0))
				fieldTag = fmt.Sprintf(`parquet:"name=%v, type=FLOAT"`, strings.ToLower(field_name))
				break
			case "BYTE_ARRAY":
				fieldTypes[i] = "BYTE_ARRAY"
				fieldType = reflect.TypeOf(string(""))
				fieldTag = fmt.Sprintf(`parquet:"name=%v, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY"`, strings.ToLower(field_name))
				break
			default:
				ErrorExit("Error: Invalid type for field %v: %v %v\n", field_name, field_type, field_type2)
			}
		}
		structFields[i] = reflect.StructField{
			Name: strings.ToUpper(field_name),
			Type: fieldType,
			Tag:  reflect.StructTag(fieldTag),
		}
		tdFieldName := checkFieldName(field_name)
		
		fields_list = addItem(fields_list, tdFieldName)
		fields_list2 = addItem(fields_list2, ":"+tdFieldName)
	}

	dataType = reflect.StructOf(structFields)

	/**************************************************************
		Create CSV file
	 **************************************************************/

	os.RemoveAll(csv_filename)

	fcsv, err = os.Create(csv_filename)
	if err != nil {
		ErrorExit("Can't create CSV file", err)
	}

	return ReadParquet(fr)

}

func ReadParquet(fr source.ParquetFile) error {

	v := reflect.New(dataType).Elem()
	pr, err := reader.NewParquetReader(fr, v.Addr().Interface(), 4)
	if err != nil {
		fmt.Printf("Can't create parquet reader: %v\n", err)
		return err
	}

	/**************************************************************
		Read Parquet File
	 **************************************************************/

	batchSize := 100
	numRows := int(pr.GetNumRows())

	// type []rowType
	sliceType := reflect.SliceOf(dataType)
	// var *[]rowType
	slicePtr := reflect.New(sliceType)
	for numRows > 0 {
		rowCount := batchSize
		if numRows < rowCount {
			rowCount = numRows
		}
		numRows -= rowCount

		// make([]rowType, rowCount, rowCount)
		slicePtr.Elem().Set(reflect.MakeSlice(sliceType, rowCount, rowCount))

		if err = pr.Read(slicePtr.Interface()); err != nil {
			fmt.Printf("Read error: %v\n", err)
			return err
		}
		// callback
		slice := slicePtr.Elem()
		for i := 0; i < slice.Len(); i++ {
			line := ""
			for j := 0; j < nFields; j++ {
				if j > 0 {
					line += ","
				}
				line += getString(slice.Index(i).Field(j), fieldChange[j])
			}
			line += "\n"
			if _, err := fcsv.WriteString(line); err != nil {
				fmt.Printf("Error writing line to CSV file: %v\n", err)
				return err
			}
		}
	}

	pr.ReadStop()
	fr.Close()
	return nil
}

func main() {

	/**************************************************************
		Parse command lines flag and arguments
	 **************************************************************/
	flag.BoolVar(&isVerbose, "v", false, "verbose mode")
	flag.Parse()

	if len(flag.Args()) !=2 {
		ErrorExit("Usage:\nparquet2csv parquet_file csv_file")
	}

	parquet_filename := flag.Arg(0)
	csv_filename := flag.Arg(1)

	err := CreateSchemaRead(parquet_filename, csv_filename)
	if err != nil {
		fmt.Printf("Error with file %v: %v", parquet_filename, err)
	}

	fcsv.Close()

}
