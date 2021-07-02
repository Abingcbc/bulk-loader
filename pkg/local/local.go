package local

import (
	"bulkloader/pkg/config"
	"context"
	"io"
	"sync"

	"github.com/pingcap/br/pkg/lightning/backend"
	"github.com/pingcap/br/pkg/lightning/backend/kv"
	"github.com/pingcap/br/pkg/lightning/common"
	"github.com/pingcap/br/pkg/lightning/mydump"
	"github.com/pingcap/br/pkg/lightning/worker"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
)

type task struct {
	taskID   int32
	filePath string
}

type batch struct {
	writer *backend.LocalEngineWriter
	kvs    []common.KvPair
}

func Sort(cfg *config.Config) {
	ctx := context.Background()
	taskCh := make(chan task, cfg.App.SortConcurrency)
	defer close(taskCh)
	store, err := storage.NewLocalStorage(cfg.Mydumper.SourceDir)
	if err != nil {
		return
	}

	w := &sync.WaitGroup{}

	restore(ctx, cfg, store, taskCh, w)

	currentTaskID := 0
	err = store.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		w.Add(1)
		taskCh <- task{
			taskID:   int32(currentTaskID),
			filePath: path,
		}
		currentTaskID++
		return nil
	})

	w.Wait()

}

func restore(ctx context.Context, cfg *config.Config, store *storage.LocalStorage, taskCh chan task, w *sync.WaitGroup) {
	sorter, err := NewLocalSorter(ctx, cfg)
	if err != nil {
		return
	}

	for i := 0; i < cfg.App.SortConcurrency; i++ {
		go func() {
			// TODO: the size of channel depends on the speed of read file and sst file write
			batchCh := make(chan batch, 4)
			defer close(batchCh)
			sortCompleteCh := make(chan struct{})

			// multiple tasks can reuse one goroutine
			go func() {
				for b := range batchCh {
					if len(b.kvs) == 0 {
						// One task completed
						sortCompleteCh <- struct{}{}
						continue
					}
					b.writer.WriteRows(ctx, []string{"key", "val"}, kv.MakeRowsFromKvPairs(b.kvs))
					if err != nil {
						return
					}
				}
			}()

			for task := range taskCh {
				reader, err := store.Open(ctx, task.filePath)
				if err != nil {
					return
				}
				parser := mydump.NewCSVParser(&cfg.Mydumper.CSV, reader, int64(cfg.Mydumper.ReadBlockSize),
					worker.NewPool(ctx, cfg.App.IOConcurrency, "io"), cfg.Mydumper.CSV.Header)
				writer, err := sorter.NewWriter(ctx, task.taskID)

				readLoop(cfg, parser, batchCh, writer)

				// no more batch for this task
				batchCh <- batch{
					kvs: make([]common.KvPair, 0),
				}
				// wait for sorting completed
				<-sortCompleteCh
				w.Done()
			}
		}()
	}
}

func readLoop(cfg *config.Config, parser *mydump.CSVParser, batchCh chan batch, writer *backend.LocalEngineWriter) {
	readEOF := false
	for !readEOF {
		canDeliver := false
		b := batch{
			writer: writer,
			kvs:    make([]common.KvPair, cfg.App.MaxBatchSize),
		}
		for !canDeliver {
			err := parser.ReadRow()
			if errors.Cause(err) == io.EOF {
				readEOF = true
				break
			}
			b.kvs = append(b.kvs, common.KvPair{
				Key: parser.LastRow().Row[0].GetBytes(),
				Val: parser.LastRow().Row[1].GetBytes(),
			})
			if len(b.kvs) >= cfg.App.MaxBatchSize {
				canDeliver = true
			}
		}
		if len(b.kvs) > 0 {
			batchCh <- b
		}
	}
}
