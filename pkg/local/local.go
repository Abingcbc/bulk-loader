package local

import (
	"bulkloader/pkg/config"
	"context"
	"fmt"

	"github.com/pingcap/br/pkg/lightning/mydump"
	"github.com/pingcap/br/pkg/lightning/worker"
	"github.com/pingcap/br/pkg/storage"
)

type task struct {
	filePath string
}

func Run(cfg *config.Config) {
	ctx := context.Background()
	taskCh := make(chan task, cfg.App.SortConcurrency)

	for i := 0; i < cfg.App.SortConcurrency; i++ {
		go func() {
			for task := range taskCh {
				s, err := storage.NewLocalStorage(cfg.Mydumper.SourceDir)
				if err != nil {
					return
				}
				reader, err := s.Open(ctx, task.filePath)
				parser := mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, int64(cfg.Mydumper.ReadBlockSize),
					worker.NewPool(ctx, cfg.App.IOConcurrency, "io"), cfg.Mydumper.CSV.Header)
				pos, rowID := parser.Pos()
				fmt.Printf("%v, %v", pos, rowID)

				// readEOF := false
				// for !readEOF {

				// }
			}
		}()
	}
}
