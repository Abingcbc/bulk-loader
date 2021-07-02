package config

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/br/pkg/lightning/common"
	brconfig "github.com/pingcap/br/pkg/lightning/config"
	"github.com/pingcap/errors"
)

var (
	// DefaultFilter for mydumper
	DefaultFilter = []string{
		"*.*",
		"!mysql.*",
		"!sys.*",
		"!INFORMATION_SCHEMA.*",
		"!PERFORMANCE_SCHEMA.*",
		"!METRICS_SCHEMA.*",
		"!INSPECTION_SCHEMA.*",
	}
)

// Config defines all configurations of a importing task.
type Config struct {
	App          BulkLoader               `toml:"bulkloader" json:"bulkloader"`
	Mydumper     brconfig.MydumperRuntime `toml:"mydumper" json:"mydumper"`
	TiDB         brconfig.DBStore         `toml:"tidb" json:"tidb"`
	Security     *brconfig.Security       `toml:"security" json:"security"`
	TikvImporter brconfig.TikvImporter    `toml:"tikv-importer" json:"tikv-importer"`

	ConfigFileContent []byte
}

type BulkLoader struct {
	SortConcurrency int `toml:"sort-concurrency" json:"sort-concurrency"`
	IOConcurrency   int `toml:"io-concurrency" json:"io-concurrency"`
	MaxBatchSize    int `toml:"max-batch-size" json:"max-batch-size"`

	SortedKVDir string `toml:"sorted-kv-dir" json:"sorted-kv-dir"`
}

func NewConfig() *Config {
	return &Config{
		App: BulkLoader{},
		Mydumper: brconfig.MydumperRuntime{
			ReadBlockSize: brconfig.ReadBlockSize,
			CSV: brconfig.CSVConfig{
				Separator:       ",",
				Delimiter:       `"`,
				Header:          true,
				NotNull:         false,
				Null:            `\N`,
				BackslashEscape: true,
				TrimLastSep:     false,
			},
			StrictFormat:  false,
			MaxRegionSize: brconfig.MaxRegionSize,
			Filter:        DefaultFilter,
			NoSchema:      true,
		},
		TikvImporter: brconfig.TikvImporter{
			MaxKVPairs:       4096,
			SendKVPairs:      32768,
			RangeConcurrency: 16,
		},
		Security: &brconfig.Security{},
	}
}

// Must should be called after LoadConfig(). If LoadConfig() returns
// any error, this function will exit the program with an appropriate exit code.
func Must(cfg *Config, err error) *Config {
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		fmt.Println("Failed to parse command flags: ", err)
		os.Exit(2)
	}
	return cfg
}

// LoadConfig reads the arguments and fills in the Config.
func LoadConfig(args []string, extraFlags func(*flag.FlagSet)) (*Config, error) {
	cfg := NewConfig()
	fs := flag.NewFlagSet("", flag.ContinueOnError)

	// if both `-c` and `-config` are specified, the last one in the command line will take effect.
	// the default value is assigned immediately after the StringVar() call,
	// so it is fine to not give any default value for `-c`, to keep the `-h` page clean.
	var configFilePath string
	fs.StringVar(&configFilePath, "c", "", "(deprecated alias of -config)")
	fs.StringVar(&configFilePath, "config", "", "bulk loader configuration file")

	if extraFlags != nil {
		extraFlags(fs)
	}

	if err := fs.Parse(args); err != nil {
		return nil, errors.Trace(err)
	}

	if len(configFilePath) > 0 {
		data, err := ioutil.ReadFile(configFilePath)
		if err != nil {
			return nil, errors.Annotatef(err, "Cannot read config file `%s`", configFilePath)
		}
		if err = toml.Unmarshal(data, cfg); err != nil {
			return nil, errors.Annotatef(err, "Cannot parse config file `%s`", configFilePath)
		}
		cfg.ConfigFileContent = data
	}

	return cfg, nil
}

func (cfg *Config) ToTLS() (*common.TLS, error) {
	fmt.Println(cfg.Security.CAPath)
	hostPort := net.JoinHostPort(cfg.TiDB.Host, strconv.Itoa(cfg.TiDB.StatusPort))
	return common.NewTLS(cfg.Security.CAPath, cfg.Security.CertPath, cfg.Security.KeyPath, hostPort)
}
