
simulate:	simulate.go
	go build -o simulate simulate.go

csv2parquet:	csv2parquet.go
	go build -o csv2parquet csv2parquet.go

parquetcsv:	parquetcsv.go
	go build -o parquetcsv parquetcsv.go

read:	read.go
	go build -o read read.go

fmt:
	go fmt ./...
