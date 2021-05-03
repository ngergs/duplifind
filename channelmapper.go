package main

import (
	"context"
	"hash/maphash"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/selfenergy/duplifind/ioutils"
)

// Maps the fileInfos from the input fileInfoChannel to a map with the filesizes as keys.
// Block till the fileInfoChannel is closed
func mapByFileSize(fileInfoChannel <-chan ioutils.FileInfoWithPath) map[int64][]string {
	result := make(map[int64][]string)
	for file := range fileInfoChannel {
		path := filepath.Join(file.Path, file.Info.Name())
		size := file.Info.Size()
		val, ok := result[size]
		if ok {
			result[size] = append(val, path)
		} else {
			slice := make([]string, 1, 8)
			slice[0] = path
			result[size] = slice
		}
	}
	return result
}

// converts the input path slice into a map obtained from reading the referenced files
// and building hashes
func mapByHash(ctx context.Context, candidates []string) map[uint64][]string {
	seed := maphash.MakeSeed()
	result := make(map[uint64][]string)
	for _, candidate := range candidates {
		hash, err := ioutils.HashFile(ctx, chunkSize, candidate, seed)
		if err != nil {
			continue
		}
		val, ok := result[hash]
		if ok {
			result[hash] = append(val, candidate)
		} else {
			slice := make([]string, 1, 8)
			slice[0] = candidate
			result[hash] = slice
		}
	}
	return result
}

// Reads the input map and forwards all results with a value array larger than 1 to the output channel
// only supports map[uint64][]string and map[int64][]string for now
// The resulting output channel is not closed upon completion of the async processing
func filterOnlyDuplicatesInternal(input interface{}, result chan<- []string /* not closed upon completion of the async processing*/) {
	defer close(result)
	switch inputMap := input.(type) {
	case map[int64][]string:
		for _, val := range inputMap {
			if len(val) > 1 {
				result <- val
			}
		}
	case map[uint64][]string:
		for _, val := range inputMap {
			if len(val) > 1 {
				result <- val
			}
		}
	default:
		log.Panic().
			Msg("Unexpected arg type, expected map[uint64][]string or map[int64][]string.")
	}
}

// Reads the input map and forwards all results with a value array larger than 1 to the output channel
// only supports map[uint64][]string and map[int64][]string for now
// The resulting output channel is closed upon completion of the async processing
func filterOnlyDuplicates(input interface{}) <-chan []string /* closed upon completion of the async processing*/ {
	result := make(chan []string, 128)
	go filterOnlyDuplicatesInternal(input, result)
	return result
}
