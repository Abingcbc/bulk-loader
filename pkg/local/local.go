package local

import (
	"bulkloader/pkg/config"
	"context"
	"io"
	"path"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/br/pkg/lightning/backend"
	"github.com/pingcap/br/pkg/lightning/backend/kv"
	"github.com/pingcap/br/pkg/lightning/common"
	"github.com/pingcap/br/pkg/lightning/mydump"
	"github.com/pingcap/br/pkg/lightning/worker"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/errors"
	clientconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
)

type task struct {
	taskID   int32
	filePath string
}

type sortBatch struct {
	writer *backend.LocalEngineWriter
	kvs    []common.KvPair
}

type putBatch struct {
	Keys [][]byte
	Vals [][]byte
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

	sorter := restore(ctx, cfg, store, taskCh, w)

	if err != nil {
		return
	}
	err = store.WalkDir(ctx, &storage.WalkOption{}, func(path string, size int64) error {
		w.Add(1)
		taskCh <- task{
			taskID:   int32(cfg.App.SortedKVID),
			filePath: path,
		}
		return nil
	})

	w.Wait()
	sorter.Close(ctx, cfg.App.SortedKVID)
	put(ctx, cfg)
}

func restore(ctx context.Context, cfg *config.Config, store *storage.LocalStorage, taskCh chan task, w *sync.WaitGroup) *Sorter {
	sorter, err := NewLocalSorter(ctx, cfg)
	if err != nil {
		return nil
	}

	for i := 0; i < cfg.App.SortConcurrency; i++ {
		go func() {
			// TODO: the size of channel depends on the speed of read file and sst file write
			batchCh := make(chan sortBatch, 4)
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
				batchCh <- sortBatch{
					kvs: make([]common.KvPair, 0),
				}
				// wait for sorting completed
				<-sortCompleteCh
				w.Done()
			}
		}()
	}

	return sorter
}

func readLoop(cfg *config.Config, parser *mydump.CSVParser, batchCh chan sortBatch, writer *backend.LocalEngineWriter) {
	readEOF := false
	for !readEOF {
		canDeliver := false
		b := sortBatch{
			writer: writer,
			kvs:    make([]common.KvPair, 0),
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

func put(ctx context.Context, cfg *config.Config) {
	putBatchCh := make(chan putBatch, cfg.App.PutConcurrency)
	w := &sync.WaitGroup{}
	for i := 0; i < cfg.App.PutConcurrency; i++ {
		go func() {
			w.Add(1)
			client, err := tikv.NewRawKVClient([]string{cfg.TiDB.PdAddr}, clientconfig.DefaultConfig().Security)
			defer client.Close()
			if err != nil {
				return
			}
			for b := range putBatchCh {
				if len(b.Keys) == 0 {
					break
				}
				client.BatchPut(b.Keys, b.Vals)
			}

			w.Done()
		}()
	}

	sendLoop(cfg, putBatchCh)

	w.Wait()
}

func sendLoop(cfg *config.Config, putBatchCh chan putBatch) {
	_, sortedKVUUID := backend.MakeUUID("", cfg.App.SortedKVID)
	db, err := pebble.Open(path.Join(cfg.TikvImporter.SortedKVDir, sortedKVUUID.String()), &pebble.Options{})
	if err != nil {
		return
	}
	var putBatchBuffer putBatch

	iter := db.NewIter(&pebble.IterOptions{})
	iter.First()
	// skip metadata
	iter.Next()
	// the bottleneck is the network when batch putting, not local reading
	for ; iter.Valid(); iter.Next() {
		currentKey := make([]byte, len(iter.Key()))
		currentVal := make([]byte, len(iter.Value()))
		copy(currentKey, iter.Key())
		copy(currentVal, iter.Value())

		putBatchBuffer.Keys = append(putBatchBuffer.Keys, currentKey)
		putBatchBuffer.Vals = append(putBatchBuffer.Vals, currentVal)
		if len(putBatchBuffer.Keys) >= 2 {
			putBatchCh <- putBatchBuffer
			putBatchBuffer = putBatch{}
		}
	}
	if len(putBatchBuffer.Keys) != 0 {
		putBatchCh <- putBatchBuffer
	}
	if err := iter.Close(); err != nil {
		return
	}
	if err := db.Close(); err != nil {
		return
	}
	// close all batch_put workers by putting empty batch
	for i := 0; i < cfg.App.PutConcurrency; i++ {
		putBatchCh <- putBatch{}
	}
}
