package local

import (
	"bulkloader/pkg/config"
	"context"
	"math"

	"github.com/pingcap/br/pkg/lightning/backend"
	brlocal "github.com/pingcap/br/pkg/lightning/backend/local"
	"github.com/pingcap/errors"
)

type Sorter struct {
	backend *backend.Backend

	taskEngineMap map[int32]*backend.OpenedEngine
}

func NewLocalSorter(ctx context.Context, cfg *config.Config) (*Sorter, error) {
	var rLimit brlocal.Rlim_t
	rLimit, err := brlocal.GetSystemRLimit()
	if err != nil {
		return nil, err
	}
	maxOpenFiles := int(rLimit / brlocal.Rlim_t(cfg.App.SortConcurrency))
	// check overflow
	if maxOpenFiles < 0 {
		maxOpenFiles = math.MaxInt32
	}

	tls, err := cfg.ToTLS()
	if err != nil {
		return nil, err
	}
	localBackend, err := brlocal.NewLocalBackend(ctx, tls, cfg.TiDB.PdAddr, &cfg.TikvImporter,
		true, nil, maxOpenFiles)
	if err != nil {
		return nil, errors.Annotate(err, "build local backend failed")
	}

	return &Sorter{
		backend:       &localBackend,
		taskEngineMap: make(map[int32]*backend.OpenedEngine),
	}, nil
}

func (s *Sorter) NewWriter(ctx context.Context, engineID int32) (*backend.LocalEngineWriter, error) {
	// TODO: fix ts
	var engine *backend.OpenedEngine
	var err error
	if _, ok := s.taskEngineMap[engineID]; ok {
		engine = s.taskEngineMap[engineID]
	} else {
		engine, err = s.backend.OpenEngine(ctx, &backend.EngineConfig{}, "", engineID, 0)
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.taskEngineMap[engineID] = engine
	}
	writer, err := engine.LocalWriter(ctx,
		&backend.LocalWriterConfig{
			IsKVSorted: false,
		})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return writer, nil
}

func (s *Sorter) Close(ctx context.Context, taskID int32) {
	if engine, ok := s.taskEngineMap[taskID]; ok {
		engine.Close(ctx)
	}
}
