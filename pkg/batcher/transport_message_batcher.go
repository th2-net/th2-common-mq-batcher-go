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
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"github.com/th2-net/th2-common-go/pkg/log"
	"github.com/th2-net/th2-common-go/pkg/queue/message"
	transport "github.com/th2-net/transport-go/pkg"
)

type Protocol = string

const (
	th2TransportProtocolAttribute = "transport-group"
)

type MessageArguments struct {
	Metadata  map[string]string
	Alias     string
	Direction Direction
	Protocol  Protocol
}

type messageBatcher struct {
	logger     zerolog.Logger
	encoder    transport.Encoder
	groupIndex int

	pipe        chan transport.RawMessage
	seqProvider sequenceProvider

	batchSizeBytes int
	flushTimeout   time.Duration

	router   message.Router
	book     string
	group    string
	protocol Protocol

	done chan bool
}

func NewMessageBatcher(router message.Router, cfg MqMessageBatcherConfig) (MqBatcher[MessageArguments], error) {
	flushTimeout := cfg.FlushMillis
	if flushTimeout <= 0 {
		flushTimeout = DefaultFlushTime
	}

	maxBatchSizeBytes := cfg.BatchSizeBytes
	if maxBatchSizeBytes <= 0 {
		maxBatchSizeBytes = DefaultBatchSize
	}

	channelSize := cfg.ChannelSize
	if channelSize <= 0 {
		channelSize = DefaultChanelSize
	}

	flushDuration := time.Duration(flushTimeout) * time.Millisecond

	batcher := messageBatcher{
		logger:         log.ForComponent(fmt.Sprintf("batcher-b(%s)-g(%s)", cfg.Book, cfg.Group)),
		encoder:        transport.NewEncoder(make([]byte, maxBatchSizeBytes)),
		pipe:           make(chan transport.RawMessage, channelSize),
		done:           make(chan bool, 1),
		seqProvider:    sequenceProvider{},
		batchSizeBytes: int(maxBatchSizeBytes),
		flushTimeout:   flushDuration,
		router:         router,
		protocol:       cfg.Protocol,
		group:          cfg.Group,
		book:           cfg.Book,
	}
	go batcher.flushingRoutine()
	return &batcher, nil
}

func (b *messageBatcher) Send(data []byte, args MessageArguments) error {
	dataLength := len(data)
	if dataLength <= 0 {
		return nil
	}

	if dataLength > int(b.batchSizeBytes) {
		return fmt.Errorf("too large message data %d, max %d", dataLength, b.batchSizeBytes)

	}

	msg := transport.RawMessage{
		MessageId: transport.MessageId{
			SessionAlias: args.Alias,
			Direction:    args.Direction,
		},
		Protocol: orDefaultIfEmpty(args.Protocol, b.protocol),
		Metadata: args.Metadata,
		Body:     data,
	}

	msgSize := transport.SizeEncodedRaw(b.group, b.book, msg)
	if msgSize > int(b.batchSizeBytes) {
		return fmt.Errorf("too large encoded message %d, max %d", msgSize, b.batchSizeBytes)

	}

	b.pipe <- msg
	return nil
}

func (b *messageBatcher) Close() error {
	b.logger.Info().Msg("Closing message batcher")
	close(b.pipe)
	<-b.done
	return nil
}

func (b *messageBatcher) flushingRoutine() {
	b.logger.Info().Msg("Flushing routine started")
	defer func() {
		b.done <- true
	}()

	timer := time.After(b.flushTimeout)
	for {
		select {
		case item, ok := <-b.pipe:
			if !ok {
				b.logger.Debug().Msg("Flushing messages by pipe complete")
				b.flush()

				b.logger.Info().Msg("Flushing routine stopped")
				return
			}
			newSize := b.encoder.SizeAfterEncodeRaw(b.group, b.book, item, b.groupIndex)
			b.logger.Trace().Int("newSize", newSize).Int("currentSize", b.batchSizeBytes).Msg("approximate batch size computed")
			if b.encoder.SizeAfterEncodeRaw(b.group, b.book, item, b.groupIndex) > b.batchSizeBytes {
				b.logger.Debug().Msg("Flushing messages by buffer size")
				b.flush()
				newSize = b.encoder.SizeAfterEncodeRaw(b.group, b.book, item, b.groupIndex)
			}

			b.write(item)
			if newSize >= b.batchSizeBytes {
				b.logger.Debug().Msg("Flushing messages by buffer is full")
				b.flush()
			}
		case <-timer:
			b.logger.Debug().Msg("Flushing messages by timer is over")
			b.flush()
			timer = time.After(b.flushTimeout)
		}
	}

}

func (b *messageBatcher) write(msg transport.RawMessage) {
	id := msg.MessageId
	id.Sequence = b.seqProvider.nextSeq(id.SessionAlias, id.Direction)
	id.Timestamp = transport.TimestampFromTime(time.Now().UTC())
	msg.MessageId = id

	b.encoder.EncodeRaw(msg, b.groupIndex)
	b.groupIndex++
}

func (b *messageBatcher) flush() {
	if b.groupIndex == 0 {
		b.logger.Trace().Msg("Flushing has skipped because buffer is empty")
		return
	}
	b.logger.Trace().Int("messageCount", b.groupIndex).Msg("Store messages")
	if err := b.router.SendRawAll(b.encoder.CompleteBatch(b.group, b.book), th2TransportProtocolAttribute); err != nil {
		b.logger.Panic().Err(err).Msg("flushing message failure")
	}

	b.reset()
}

func (b *messageBatcher) reset() {
	b.logger.Trace().Msg("Resetting state")

	b.encoder.Reset()
	b.groupIndex = 0
}
