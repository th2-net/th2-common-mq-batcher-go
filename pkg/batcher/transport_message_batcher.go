/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package batcher

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/th2-net/th2-common-go/pkg/queue/message"
)

type protocol = string
type metadata = map[string]string
type codecType = uint8

const (
	long_codec_type          codecType = 1
	string_codec_type        codecType = 2
	message_id_codec_type    codecType = 10
	book_codec_type          codecType = 101
	session_group_codec_type codecType = 102
	session_alias_codec_type codecType = 103
	direction_codec_type     codecType = 104
	sequence_codec_type      codecType = 105
	subsequence_codec_type   codecType = 106
	timestamp_codec_type     codecType = 107
	metadata_codec_type      codecType = 11
	protocol_codec_type      codecType = 12
	raw_message_codec_type   codecType = 20
	raw_body_codec_type      codecType = 21
	message_group_codec_type codecType = 40
	message_list_codec_type  codecType = 41
	group_batch_codec_type   codecType = 50
	group_list_codec_type    codecType = 51

	length_size int = 4

	th2_pin_attribute string = "transport-group"
)

type MessageArguments struct {
	Metadata  map[string]string
	Alias     string
	Direction Direction
	Protocol  protocol
}

type messageBatcher struct {
	buffer      []byte
	bufferIndex int

	batchLenIndex  int
	groupsLenIndex int
	groupIndex     int

	pipe        chan container
	seqProvider sequenceProvider

	batchSizeBytes uint
	flushTimeout   time.Duration

	router   message.Router
	book     string
	group    string
	protocol protocol

	done chan bool
}

func NewMessageBatcher(router message.Router, cfg MqMessageBatcherConfig) (MqBatcher[MessageArguments], error) {
	flushTimeout := cfg.FlushMillis
	if flushTimeout <= 0 {
		flushTimeout = DEFAULT_FLUSH_TIME
	}

	maxBatchSizeBytes := cfg.BatchSizeBytes
	if maxBatchSizeBytes <= 0 {
		maxBatchSizeBytes = DEFAULT_BATCH_SIZE
	}

	flushDuration := time.Duration(flushTimeout) * time.Millisecond

	batcher := messageBatcher{
		buffer:         make([]byte, maxBatchSizeBytes),
		pipe:           make(chan container, 1),
		done:           make(chan bool, 1),
		seqProvider:    sequenceProvider{},
		batchSizeBytes: maxBatchSizeBytes,
		flushTimeout:   flushDuration,
		router:         router,
		protocol:       cfg.Protocol,
		group:          cfg.Group,
		book:           cfg.Book,
	}
	batcher.reset() // reset flushing time for first message
	go batcher.flushingRoutine()
	return &batcher, nil
}

func (b *messageBatcher) Send(data []byte, args MessageArguments) (int, error) {
	dataLength := len(data)
	if dataLength <= 0 {
		return 0, nil
	}

	overhead := b.computeOverhead(data, args)
	if overhead > int(b.batchSizeBytes) {
		return overhead, fmt.Errorf("too large message of data %d, serialized size %d, max %d", dataLength, overhead, b.batchSizeBytes)

	}

	b.pipe <- container{data, args, overhead}

	return overhead, nil
}

func (b *messageBatcher) Close() error {
	log.Info().Msg("Closing message batcher")
	close(b.pipe)
	<-b.done
	return nil
}

func (b *messageBatcher) flushingRoutine() {
	log.Info().Msg("Flushing routine started")
	defer func() {
		b.done <- true
	}()
	for {
		timer := time.After(b.flushTimeout)

		select {
		case item, ok := <-b.pipe:
			if !ok {
				log.Debug().Msg("Flushing messages by pipe complete")
				b.flush()

				log.Info().Msg("Flushing routine stopped")
				return
			}
			if b.bufferIndex+item.overhead > cap(b.buffer) {
				log.Debug().Msg("Flushing messages by buffer size")
				b.flush()
			}

			b.write(item.data, item.args.Metadata, item.args.Alias, item.args.Direction, item.args.Protocol)
			if b.bufferIndex == cap(b.buffer) {
				log.Debug().Msg("Flushing messages by buffer is full")
				b.flush()
			}
		case <-timer:
			log.Debug().Msg("Flushing messages by timer is over")
			b.flush()
		}
	}

}

func (b *messageBatcher) write(data []byte, metadata map[string]string, alias string, direction Direction, protocol string) {
	dst := b.buffer[b.bufferIndex:]
	start := 0
	// group
	start += writeType(dst[start:], message_group_codec_type)
	msgGroupLenIndex := start
	start += writeLen(dst[start:], 0)

	// list
	start += writeType(dst[start:], message_list_codec_type)
	msgListLenIndex := start
	start += writeLen(dst[start:], 0)

	// msg
	start += writeType(dst[start:], raw_message_codec_type)
	msgLenIndex := start
	start += writeLen(dst[start:], 0)

	// msg id
	start += writeType(dst[start:], message_id_codec_type)
	msgIdLenIndex := start
	start += writeLen(dst[start:], 0)

	start += writeString(dst[start:], session_alias_codec_type, alias)
	start += writeDirection(dst[start:], direction)
	sequence := b.seqProvider.nextSeq(alias, direction)
	start += writeLongValue(dst[start:], sequence_codec_type, sequence)
	start += writeLongCollection(dst[start:], subsequence_codec_type, nil)
	start += writeTime(dst[start:], time.Now().UnixNano())
	writeLen(dst[msgIdLenIndex:], start-msgIdLenIndex-length_size)

	// metadata
	start += writeMetadata(dst[start:], metadata)

	// protocol
	start += writeProtocol(dst[start:], orDefaultIfEmpty(protocol, b.protocol))

	// body
	start += writeType(dst[start:], raw_body_codec_type)
	start += writeLen(dst[start:], len(data))
	start += copy(dst[start:], data)

	writeLen(dst[msgLenIndex:], start-msgLenIndex-length_size)
	writeLen(dst[msgListLenIndex:], start-msgListLenIndex-length_size)
	writeLen(dst[msgGroupLenIndex:], start-msgGroupLenIndex-length_size)

	b.bufferIndex += start

	b.groupIndex++
}

func (b *messageBatcher) flush() {
	if b.groupIndex == 0 {
		log.Trace().Msg("Flushing has skipped because buffer is empty")
		return
	}
	log.Trace().Int("messageCount", b.groupIndex).Msg("Store messages")
	batchLen := b.bufferIndex - b.batchLenIndex - length_size
	messagesLen := b.bufferIndex - b.groupsLenIndex - length_size
	writeLen(b.buffer[b.batchLenIndex:], batchLen)
	writeLen(b.buffer[b.groupsLenIndex:], messagesLen)

	if err := b.router.SendRawAll(b.buffer[:b.bufferIndex], th2_pin_attribute); err != nil {
		log.Panic().Err(err).Msg("flushing message failure")
	}

	b.reset()
}

func (b *messageBatcher) reset() {
	log.Trace().Int("bufferIndex", b.bufferIndex).Msg("Resetting state")

	b.groupIndex = 0
	b.batchLenIndex = 0
	b.groupsLenIndex = 0
	b.bufferIndex = 0
	b.bufferIndex += writeType(b.buffer[b.bufferIndex:], group_batch_codec_type)
	b.batchLenIndex = b.bufferIndex
	b.bufferIndex += writeLen(b.buffer[b.bufferIndex:], 0)

	b.bufferIndex += writeBook(b.buffer[b.bufferIndex:], b.book)
	b.bufferIndex += writeGroup(b.buffer[b.bufferIndex:], b.group)

	b.bufferIndex += writeType(b.buffer[b.bufferIndex:], group_list_codec_type)
	b.groupsLenIndex = b.bufferIndex
	b.bufferIndex += writeLen(b.buffer[b.bufferIndex:], 0)
}

func (b *messageBatcher) computeOverhead(data []byte, args MessageArguments) int {
	groupSize := 1 + 4
	messageListSize := 1 + 4
	messageSize := 1 + 4
	aliasSize := 1 + 4 + len(args.Alias)
	messageIdSize := (1 + 4) + aliasSize + (1 + 4 + 1) + (1 + 4 + 8) + (1 + 4) + (1 + 4 + 8 + 4)
	metadataSize := 1 + 4
	protocolSize := 1 + 4 + len(orDefaultIfEmpty(args.Protocol, b.protocol))
	bodySize := 1 + 4 + len(data)
	return groupSize + messageListSize + messageSize + messageIdSize + metadataSize + protocolSize + bodySize
}

func writeType(dest []byte, t codecType) int {
	dest[0] = t
	return 1
}

func writeLen(dest []byte, len int) int {
	return writeInt(dest, len)
}

func writeInt(dest []byte, value int) int {
	binary.LittleEndian.PutUint32(dest, uint32(value))
	return 4
}

func writeLong(dest []byte, value int64) int {
	binary.LittleEndian.PutUint64(dest, uint64(value))
	return 8
}

func writeLongValue(dest []byte, t codecType, value int64) int {
	start := 0
	start += writeType(dest[start:], t)
	start += writeLen(dest[start:], 8)
	start += writeLong(dest[start:], value)
	return start
}

func writeDirection(dest []byte, dir Direction) int {
	start := 0
	start += writeType(dest[start:], direction_codec_type)
	start += writeLen(dest[start:], 1)
	dest[start] = byte(dir)
	return start + 1
}

func writeString(dest []byte, t codecType, value string) int {
	start := 0
	start += writeType(dest[start:], t)
	start += writeLen(dest[start:], len(value))
	start += copy(dest[start:], value)
	return start
}

func writeBook(dest []byte, book string) int {
	return writeString(dest, book_codec_type, book)
}

func writeGroup(dest []byte, group string) int {
	return writeString(dest, session_group_codec_type, group)
}

func writeProtocol(dest []byte, protocol protocol) int {
	return writeString(dest, protocol_codec_type, protocol)
}

func writeMetadata(dest []byte, metadata metadata) int {
	start := 0
	start += writeType(dest[start:], metadata_codec_type)
	lenIndex := start
	start += writeLen(dest[start:], 0)
	for k, v := range metadata {
		start += writeString(dest[start:], string_codec_type, k)
		start += writeString(dest[start:], string_codec_type, v)
	}
	writeLen(dest[lenIndex:], start-lenIndex-length_size)
	return start
}

func writeTime(dest []byte, time int64) int {
	start := 0
	start += writeType(dest[start:], timestamp_codec_type)
	start += writeLen(dest[start:], 8+4)
	start += writeLong(dest[start:], time/1_000_000_000)
	start += writeInt(dest[start:], int(time%1_000_000_000))
	return start
}

func writeLongCollection(dest []byte, t codecType, data []int64) int {
	start := 0
	start += writeType(dest[start:], t)
	lenIndex := start
	start += writeLen(dest[start:], 0)
	for _, v := range data {
		start += writeLongValue(dest[start:], long_codec_type, v)
	}
	writeLen(dest[lenIndex:], start-lenIndex-length_size)
	return start
}
