//Origin: https://github.com/xitongsys/parquet-go/blob/master/example/convert_to_json.go
package main

import (
	"flag"
	"encoding/json"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
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
	
	num := int(pr.GetNumRows())
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

	fmt.Printf("%s\n",jsonBs)

	pr.ReadStop()
	fr.Close()

}