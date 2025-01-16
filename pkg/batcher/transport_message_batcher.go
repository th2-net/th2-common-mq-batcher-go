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

	"github.com/rs/zerolog/log"
	"github.com/th2-net/th2-common-go/pkg/queue/message"
	transport "github.com/th2-net/transport-go/pkg"
)

type protocol = string

const (
	th2_pin_attribute string = "transport-group"
)

type MessageArguments struct {
	Metadata  map[string]string
	Alias     string
	Direction Direction
	Protocol  protocol
}

type messageBatcher struct {
	encoder    transport.Encoder
	groupIndex int

	pipe        chan transport.RawMessage
	seqProvider sequenceProvider

	batchSizeBytes int
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
		encoder:        transport.NewEncoder(make([]byte, maxBatchSizeBytes)),
		pipe:           make(chan transport.RawMessage, 1),
		done:           make(chan bool, 1),
		seqProvider:    sequenceProvider{},
		batchSizeBytes: int(maxBatchSizeBytes),
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

func (b *messageBatcher) Send(data []byte, args MessageArguments) error {
	dataLength := len(data)
	if dataLength <= 0 {
		return nil
	}

	if dataLength > int(b.batchSizeBytes) {
		return fmt.Errorf("too large message of data %d, max %d", dataLength, b.batchSizeBytes)

	}

	b.pipe <- transport.RawMessage{
		MessageId: transport.MessageId{
			SessionAlias: args.Alias,
			Direction:    args.Direction,
		},
		Protocol: orDefaultIfEmpty(args.Protocol, b.protocol),
		Metadata: args.Metadata,
		Body:     data,
	}

	return nil
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
			newSize := b.encoder.SizeAfterEncodeRaw(b.group, b.book, item, b.groupIndex)
			log.Trace().Msgf("Size after encoding: %d", newSize)
			if b.encoder.SizeAfterEncodeRaw(b.group, b.book, item, b.groupIndex) > b.batchSizeBytes {
				log.Debug().Msg("Flushing messages by buffer size")
				b.flush()
			}

			b.write(item)
			if newSize >= b.batchSizeBytes {
				log.Debug().Msg("Flushing messages by buffer is full")
				b.flush()
			}
		case <-timer:
			log.Debug().Msg("Flushing messages by timer is over")
			b.flush()
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

func (b *messageBatcher) completeBatch() []byte {
	return b.encoder.CompleteBatch(b.group, b.book)
}

func (b *messageBatcher) flush() {
	if b.groupIndex == 0 {
		log.Trace().Msg("Flushing has skipped because buffer is empty")
		return
	}
	log.Trace().Int("messageCount", b.groupIndex).Msg("Store messages")
	if err := b.router.SendRawAll(b.encoder.CompleteBatch(b.group, b.book), th2_pin_attribute); err != nil {
		log.Panic().Err(err).Msg("flushing message failure")
	}

	b.reset()
}

func (b *messageBatcher) reset() {
	log.Trace().Msg("Resetting state")

	b.encoder.Reset()
	b.groupIndex = 0
}
