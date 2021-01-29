//Ref: https://github.com/xitongsys/parquet-go/blob/master/example/convert_to_json.go, 
// https://github.com/xitongsys/parquet-go/blob/4c59bed5d5a62d392c5cbfb53482dbc6686ff238/tool/parquet-tools/parquet-tools.go
package main

import (
	"flag"
	"encoding/json"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"	
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/sizetool"
	"fmt"
	"os"
)

var (
	isVerbose               bool
)

func Debug(format string, a ...interface{}) {
	if isVerbose {
		fmt.Printf(format+"\n", a...)
	}
}

func ErrorExit(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

func main() {

	flag.BoolVar(&isVerbose, "v", false, "verbose mode")
	flag.Parse()

	if len(flag.Args()) != 1 {
		ErrorExit("Usage:\nshow parquet_file")
	}

	parquet_filename := flag.Arg(0)
	
	fr, err := local.NewLocalFileReader(parquet_filename)
	if err != nil {
		ErrorExit("Can't open file %v: %v", parquet_filename,err)		
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		ErrorExit("Can't create parquet reader: %v", err)
		return
	}

	// Schema
	withTags:=true
	tree := schematool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)
	fmt.Println("----- Go struct -----")
	fmt.Printf("%s\n", tree.OutputStruct(withTags))	
	fmt.Println("----- Json schema -----")
	fmt.Printf("%s\n", tree.OutputJsonSchema())

	// Rows count
	num := int(pr.GetNumRows())
	fmt.Println("----- Rows -----")
	fmt.Printf("%v\n", pr.GetNumRows())
	fmt.Println()

	// File size
	withPrettySize := true
	sizeCompressed := sizetool.GetParquetFileSize(parquet_filename, pr, withPrettySize, false)
	sizeUncompressed := sizetool.GetParquetFileSize(parquet_filename, pr, withPrettySize, true)
	fmt.Println("----- Size -----")
	fmt.Printf("Compressed: %v\n", sizeCompressed)
	fmt.Printf("Uncompressed: %v\n", sizeUncompressed)
	fmt.Println()

	// JSON content	
	res, err := pr.ReadByNumber(num)
	if err != nil {
		ErrorExit("Can't read: %v", err)
		return
	}

	jsonBs, err := json.Marshal(res)
	if err != nil {
		ErrorExit("Can't to json: %v", err)
		return
	}

	fmt.Println("Content:")
	fmt.Printf("%s\n",jsonBs)
	fmt.Println()

	pr.ReadStop()
	fr.Close()

}