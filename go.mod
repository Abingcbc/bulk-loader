module bulkloader

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/cockroachdb/pebble v0.0.0-20201023120638-f1224da22976
	github.com/google/uuid v1.1.1
	github.com/pingcap/br v5.2.0-alpha.0.20210611153635-74f18bcbe19d+incompatible
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
)

replace github.com/pingcap/br => ../../pkg/mod/github.com/pingcap/br@v5.0.2+incompatible
