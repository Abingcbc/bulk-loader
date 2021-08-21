# bulk-loader
A raw KV importer for TiKV

* `cmd` & `pkg`: importer
* `demo` & `rawkv`: a naive example of Spark operation for BatchPut

## How to run
### Importer
1. Rename `go.mod1` in `../../pkg/mod/github.com/pingcap/br@v5.0.2+incompatible` to `go.mod` (Because `BR` has some dependency conflicts with `TiDB`)
2. `go build ./cmd/main.go`
3. `./main -c example-config.toml`
### Spark Operation Demo
1. `./build.sh`
2. `spark-submit --master "<your master>" --class Main "<your repo path>/demo/target/demo-1.0-SNAPSHOT.jar"`
