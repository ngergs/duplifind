package ioutils

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/selfenergy/duplifind/asyncutils"
)

type FileInfoWithPath struct {
	Path string
	Info os.FileInfo
}

// Reads a Directory specified by the path argument and streams
// the resulting fileDirEntry chunked in arrays of size up to 128
// to the result channel. The result channel is closed upon completion.
func readDir(path string, result chan<- []fs.DirEntry /* closed upon completion*/) {
	dir, err := os.Open(path)
	if err != nil {
		log.Warn().Stack().
			Err(errors.Wrap(err, "Failed to open file handle")).
			Msg("")
	}
	go func() {
		defer dir.Close()
		defer close(result)
		for files, err := dir.ReadDir(128); err != io.EOF; files, err = dir.ReadDir(128) {
			if err != nil {
				log.Warn().Stack().
					Err(errors.Wrap(err, "Failed to read directory infos")).
					Msg("")
				break
			}
			result <- files
		}
	}()
}

// Reads all fileInfos from a Directory and streams them to the result channel.
// Directories are followed recursively, links are ignored.
// The result channel is not closed upon completion.
// The waiting group will be decremented by 1 when everything has finished
func walkInternal(path string, result chan<- FileInfoWithPath, wg *sync.WaitGroup) {
	defer wg.Done()
	filesChunks := make(chan []fs.DirEntry, 128)
	readDir(path, filesChunks)
	for files := range filesChunks {
		for _, file := range files {
			if file.IsDir() {
				wg.Add(1)
				go walkInternal(filepath.Join(path, file.Name()), result, wg)
				continue
			}
			fileInfo, err := file.Info()
			if err != nil {
				log.Warn().Stack().
					Err(errors.Wrap(err, "Failed to read file infos")).
					Msg("")
			}
			result <- FileInfoWithPath{Path: path, Info: fileInfo}
		}
	}
}

// Reads all fileInfos from a Directory and streams them to the result channel.
// Directories are followed recursively, links are ignored.
// The returned channel is closed upon completion of the async processing.
func Walk(path string) <-chan FileInfoWithPath /* closed upon completion of the async processing*/ {
	result := make(chan FileInfoWithPath, 1024)
	go asyncutils.GoAndWait(
		func(wg *sync.WaitGroup) { walkInternal(path, result, wg) },
		func() { close(result) })
	return result
}
