package ioutils

import (
	"context"
	"io"
	"os"
)

type readerWithChannel struct {
	NamedChannel NamedByteChunkChannel
	file         *os.File
	reader       io.Reader
}

type ParallelReader struct {
	readers []readerWithChannel
}

// opens readers for the files found in the paths arguments and returns the constructed ParallelReader
// skips files where errors occur
func NewParallelReader(paths []string) ParallelReader {
	readers := make([]readerWithChannel, 0, len(paths))
	for _, path := range paths {
		file, reader, err := ReadFile(path)
		if err == nil {
			readers = append(readers, readerWithChannel{NamedChannel: NamedByteChunkChannel{Name: path, Channel: make(chan []byte)}, file: file, reader: reader})
		}
	}
	return ParallelReader{readers: readers}
}

// returns the number of successfull opened files
func (parallelReader *ParallelReader) Length() int {
	return len(parallelReader.readers)
}

// returns the opened channels
func (parallelReader *ParallelReader) GetChannels() []NamedByteChunkChannel {
	result := make([]NamedByteChunkChannel, parallelReader.Length())
	for i, el := range parallelReader.readers {
		result[i] = el.NamedChannel
	}
	return result
}

// starts reading from the files wrapped in the parallelReader
// closes the result channels as well as the wrapped files
func (parallelReader *ParallelReader) Close() {
	for _, reader := range parallelReader.readers {
		reader.NamedChannel.Close()
		reader.file.Close()
	}
}

// starts reading from the files wrapped in the parallelReader
// closes the result channels as well as the wrapped files
func (parallelReader *ParallelReader) GoReadingAndClose(ctx context.Context, chunkSize int) {
	for _, reader := range parallelReader.readers {
		go func(reader readerWithChannel) {
			defer reader.NamedChannel.Close()
			defer reader.file.Close()
			subFunc := func(data []byte) {
				select {
				case reader.NamedChannel.Channel <- data:
				case <-ctx.Done():
					return
				}
			}
			ReadFromFile(ctx, reader.reader, chunkSize, subFunc)
		}(reader)
	}
}
