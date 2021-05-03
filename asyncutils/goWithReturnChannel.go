package asyncutils

import (
	"sync"
)

// Executes the function fn in a coroutines spawnCount times and passes in a sync.WaitGroup.
// The WaitGroup is initialized with Add(spawnCount) and should be accordingly marked as Done in the passed function fn.
// Does not block and returns a string result channel that holds the results
func GoMultipleStringResult(spawnCount int, resultBufferSize int, fn func(*sync.WaitGroup, chan<- []string)) <-chan []string {
	result := make(chan []string, resultBufferSize)
	go func() {
		defer close(result)
		var wg sync.WaitGroup
		wg.Add(spawnCount)
		for i := 0; i < spawnCount; i++ {
			go fn(&wg, result)
		}
		wg.Wait()
	}()
	return result
}
