# bulk-loader
A raw KV importer for TiKV

* `cmd` & `pkg`: importer
* `demo` & `rawkv`: a naive example of Spark operation for BatchPut

## How to run
* Importer: `go build ./cmd/main.go`
* Spark Operation: `./build.sh`
