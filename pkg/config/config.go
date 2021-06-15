package config

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
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
	App      BulkLoader               `toml:"bulkloader" json:"bulkloader"`
	Mydumper brconfig.MydumperRuntime `toml:"mydumper" json:"mydumper"`

	ConfigFileContent []byte
}

type BulkLoader struct {
	SortConcurrency int `toml:"sort-concurrency" json:"sort-concurrency"`
	IOConcurrency   int `toml:"io-concurrency" json:"io-concurrency"`
}

func NewConfig() *Config {
	return &Config{
		App: BulkLoader{},
		Mydumper: brconfig.MydumperRuntime{
			ReadBlockSize: ReadBlockSize,
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
			MaxRegionSize: MaxRegionSize,
			Filter:        DefaultFilter,
			NoSchema:      true,
		},
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
