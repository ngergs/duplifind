package ioutils

import (
	"bufio"
	"context"
	"hash/maphash"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// Opens a file using os.Open and creates an io.Reader using bufio from it
// Returns nil, nil, error in case of error
func ReadFile(path string) (*os.File, io.Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		log.Warn().Stack().
			Err(errors.Wrap(err, "Could not open file for reading")).
			Msg("")
		return nil, nil, err
	}
	return f, bufio.NewReader(f), err
}

// Opens a file using os.OpenFile with modes RDWD, CRATE and APPEND; chmod 0666.
// Creates an io.Writer using bufio from it
// Returns nil, nil, error in case of error
func WriteAppendFile(path string) (*os.File, *bufio.Writer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Warn().Stack().
			Err(errors.Wrap(err, "error opening file")).
			Msg("")
		return nil, nil, err
	}
	return f, bufio.NewWriter(f), err
}

// Opens a file using os.Open and reads it in chunks, calling the passed callbackFunction for further processing
// Returns nil, nil, error in case of error
func ReadFromFile(ctx context.Context, reader io.Reader, chunkSize int, callbackFunction func([]byte)) {
	var err error = nil
	for err != io.ErrUnexpectedEOF {
		buf := make([]byte, 0, chunkSize)
		readCount, err := io.ReadFull(reader, buf[:chunkSize])
		if err == io.EOF {
			return
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			log.Warn().Stack().
				Err(errors.Wrap(err, "Could not read file content")).
				Msg("")
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
			callbackFunction(buf[:readCount])
		}
	}
}

// reads a file from disk in chunks and computes its hash using maphash
func HashFile(ctx context.Context, chunkSize int, path string, seed maphash.Seed) (uint64, error) {
	var hash maphash.Hash
	hash.SetSeed(seed)
	file, reader, err := ReadFile(path)
	defer file.Close()
	if err != nil {
		return 0, err
	}
	ReadFromFile(ctx, reader, chunkSize, func(data []byte) { hash.Write(data) })
	return hash.Sum64(), nil
}
