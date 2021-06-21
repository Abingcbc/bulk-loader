package local

import (
	"bulkloader/pkg/common"
	"bulkloader/pkg/config"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/pingcap/br/pkg/lightning/mydump"
	"github.com/pingcap/br/pkg/lightning/worker"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
)

type task struct {
	filePath string
}

func Sort(cfg *config.Config) {
	ctx := context.Background()
	taskCh := make(chan task, cfg.App.SortConcurrency)
	defer close(taskCh)

	w := &sync.WaitGroup{}

	for i := 0; i < cfg.App.SortConcurrency; i++ {
		go func() {
			// TODO: the size of channel depends on the speed of read file and pebble batch write
			batchCh := make(chan []common.KvPair, 4)
			defer close(batchCh)
			sortCompleteCh := make(chan struct{})

			// multiple tasks can reuse one goroutine
			go func() {
				for batch := range batchCh {
					if len(batch) == 0 {
						return
					}
					fmt.Println(batch)
					// One task completed
					sortCompleteCh <- struct{}{}
				}
			}()

			for task := range taskCh {
				s, err := storage.NewLocalStorage(cfg.Mydumper.SourceDir)
				if err != nil {
					fmt.Println(err.Error())
					return
				}

				reader, err := s.Open(ctx, task.filePath)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				parser := mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, int64(cfg.Mydumper.ReadBlockSize),
					worker.NewPool(ctx, cfg.App.IOConcurrency, "io"), cfg.Mydumper.CSV.Header)

				readLoop(cfg, parser, batchCh)
				batchCh <- make([]common.KvPair, 0)
				// wait for sorting completed
				<-sortCompleteCh
				w.Done()
			}
		}()
	}

	w.Add(1)
	taskCh <- task{
		filePath: "test.csv",
	}

	w.Wait()

}

func readLoop(cfg *config.Config, parser *mydump.CSVParser, batchCh chan []common.KvPair) {
	readEOF := false
	for !readEOF {
		canDeliver := false
		batch := make([]common.KvPair, 0, cfg.App.MaxBatchSize)
		for !canDeliver {
			err := parser.ReadRow()
			if errors.Cause(err) == io.EOF {
				readEOF = true
				break
			}
			batch = append(batch, common.KvPair{
				Key:   parser.LastRow().Row[0].GetBytes(),
				Value: parser.LastRow().Row[1].GetBytes(),
			})
			if len(batch) >= cfg.App.MaxBatchSize {
				canDeliver = true
			}
		}
		if len(batch) > 0 {
			batchCh <- batch
		}
	}
}
