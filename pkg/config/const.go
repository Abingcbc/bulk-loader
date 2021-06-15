package config

import "github.com/docker/go-units"

const (
	// mydumper
	ReadBlockSize   ByteSize = 64 * units.KiB
	MinRegionSize   ByteSize = 256 * units.MiB
	MaxRegionSize   ByteSize = 256 * units.MiB
	SplitRegionSize ByteSize = 96 * units.MiB

	BufferSizeScale = 5

	defaultMaxAllowedPacket = 64 * units.MiB

	defaultBatchSize ByteSize = 100 * units.GiB
)
