all:	simulate csv2parquet parquetcsv show

simulate:	simulate.go
	go build -o simulate simulate.go

csv2parquet:	csv2parquet.go
	go build -o csv2parquet csv2parquet.go

parquetcsv:	parquet2csv.go
	go build -o parquet2csv parquet2csv.go

show:	show.go
	go build -o show show.go

fmt:
	go fmt ./...
