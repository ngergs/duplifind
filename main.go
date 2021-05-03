package main

import (
	"context"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"github.com/selfenergy/duplifind/asyncutils"
	"github.com/selfenergy/duplifind/ioutils"
)

const chunkSize int = 104856

type PathAndHash struct {
	Path string
	Hash uint64
}

type ReaderWithChannel struct {
	Path    string
	File    *os.File
	Reader  io.Reader
	Channel chan []byte
}

// creates coroutine processors that hash the files referenced in the input channel and returns the relevant duplicate candidates to the returned result channel
func createHashProcessors(ctx context.Context, input <-chan []string, spawnCount int) <-chan []string {
	return asyncutils.GoMultipleStringResult(spawnCount, 1024, func(wg *sync.WaitGroup, result chan<- []string) {
		defer wg.Done()
		for candidates := range input {
			contentHashMap := mapByHash(ctx, candidates)
			duplicateByHashChan := filterOnlyDuplicates(contentHashMap)
			for candidateByHash := range duplicateByHashChan {
				result <- candidateByHash
			}
		}
	})
}

// creates coroutine procesors that check the files referenced in the input bytewise  and returns the relevant duplicate candidates to the returned result channel
func createBytewiseCheckProcessors(ctx context.Context, input <-chan []string, spawnCount int) <-chan []string {
	return asyncutils.GoMultipleStringResult(spawnCount, 1024, func(wg *sync.WaitGroup, result chan<- []string) {
		defer wg.Done()
		for candidates := range input {
			ioutils.CheckBytewiseIdentity(ctx, chunkSize, candidates, result)
		}
	})
}

func main() {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	ctx := context.Background()

	fileInfoChan := ioutils.Walk(".")
	sizeMap := mapByFileSize(fileInfoChan)
	duplicateBySizeChan := filterOnlyDuplicates(sizeMap)
	duplicateByHashChan := createHashProcessors(ctx, duplicateBySizeChan, 16)
	duplicateChan := createBytewiseCheckProcessors(ctx, duplicateByHashChan, 4)
	if err := ioutils.AppendChannelToFile("duplifind_result.csv", duplicateChan); err != nil {
		log.Fatal().
			Err(errors.Wrap(err, "error writing result")).
			Msg("")
	}
}
