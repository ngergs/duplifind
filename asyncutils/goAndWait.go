package asyncutils

import (
	"sync"
)

// Executes the function fn in a coroutines and passes in a sync.WaitGroup.
// The WaitGroup is initialized with Add(1) and should be accordingly marked as Done in the passed function fn.
// The finishCallback is called once the WaitGroup resolves.
// Should be called in a seperate coroutine as it blocks till the WaitGroup resolves.
func GoAndWait(fn func(*sync.WaitGroup), finishCallback func()) {
	defer finishCallback()
	var wg sync.WaitGroup
	wg.Add(1)
	go fn(&wg)
	wg.Wait()
}

// Executes the function fn in a coroutines spawnCount times and passes in a sync.WaitGroup.
// The WaitGroup is initialized with Add(spawnCount) and should be accordingly marked as Done in the passed function fn.
// The finishCallback is called once the WaitGroup resolves.
// Should be called in a seperate coroutine as it blocks till the WaitGroup resolves.
func GoMultipleAndWait(spawnCount int, fn func(*sync.WaitGroup), finishCallback func()) {
	defer finishCallback()
	var wg sync.WaitGroup
	wg.Add(spawnCount)
	for i := 0; i < spawnCount; i++ {
		go fn(&wg)
	}
	wg.Wait()
}

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
