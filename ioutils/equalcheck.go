package ioutils

import (
	"bytes"
	"context"
	"sync"
)

// group all readers with identical byte content obtained from the wrapped channels
// returns any duplicates found via the resultChannel
func groupIdenticalReturnDuplicates(ctx context.Context, cancelWg *sync.WaitGroup, readers []NamedByteChunkChannel, resultChan chan<- []string) {
	defer cancelWg.Done()
	if len(readers) < 1 {
		return
	}

	originalBytes, ok := <-readers[0].Channel
	if !ok {
		// channel closed, duplicate found?
		if len(readers) > 1 {
			result := make([]string, len(readers))
			for i, reader := range readers {
				result[i] = reader.Name
			}
			resultChan <- result
		}
		return
	}

	nonEqualEntriesIndex := make([]int, 0)
	for i := 1; i < len(readers); i++ {
		newBytes := <-readers[i].Channel
		eq := bytes.Equal(originalBytes, newBytes)
		if !eq {
			nonEqualEntriesIndex = append(nonEqualEntriesIndex, i)
		}
	}
	splittedReaders := Split(readers, nonEqualEntriesIndex)
	for _, splittReaders := range splittedReaders {
		if len(splittReaders) > 1 {
			cancelWg.Add(1)
			go groupIdenticalReturnDuplicates(ctx, cancelWg, splittReaders, resultChan)
		}
	}
}

// cheks the bytewise identity of the files stored under the given paths and returns any duplicates via the result channel
// the files are read in chunks using chunkSize bytes.
func CheckBytewiseIdentity(ctx context.Context, chunkSize int, paths []string, result chan<- []string) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	readers := NewParallelReader(paths)
	if readers.Length() < 2 {
		readers.Close()
		return
	}
	readers.GoReadingAndClose(ctx, chunkSize)
	var cancelWg sync.WaitGroup
	cancelWg.Add(1)
	groupIdenticalReturnDuplicates(ctx, &cancelWg, readers.GetChannels(), result)
	cancelWg.Wait()
}
