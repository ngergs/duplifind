package ioutils

import (
	"strings"

	"github.com/rs/zerolog/log"
)

type NamedByteChunkChannel struct {
	Name    string
	Channel chan []byte
}

func (namedChannel *NamedByteChunkChannel) Close() {
	close(namedChannel.Channel)
}

// Splits the input slice into two slices. The indizes passed as second argument select the elements of the first result slice,
// the remaining indizes are used to form the seonc result slice.
func Split(channels []NamedByteChunkChannel, indizes []int) [2][]NamedByteChunkChannel {
	i := 0
	for _, index := range indizes {
		i++
		channels[len(channels)-i], channels[index] = channels[index], channels[len(channels)-i]
	}
	return [2][]NamedByteChunkChannel{channels[:len(channels)-i], channels[len(channels)-i:]}
}

// writes the string arrays from the input channel to the given output file.
// blocks till the input channel is closed
func AppendChannelToFile(path string, input <-chan []string) error {
	file, writer, err := WriteAppendFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	defer writer.Flush()
	debugMsg := "Append " + path
	for elements := range input {
		joinedElement := strings.Join(elements, ",")
		log.Debug().Str(debugMsg, joinedElement).Msg("")
		writer.WriteString(joinedElement + "\n")
	}
	return nil
}
