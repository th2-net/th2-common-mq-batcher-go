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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	proto "github.com/th2-net/th2-common-go/pkg/common/grpc/th2_grpc_common"
	"github.com/th2-net/th2-common-go/pkg/queue"
	mq "github.com/th2-net/th2-common-go/pkg/queue/message"
	"github.com/th2-net/th2-common-mq-batcher-go/pkg/batcher"
	transport "github.com/th2-net/transport-go/pkg"
)

const (
	book     = "book"
	group    = "group"
	alias    = "alias"
	protocol = "protocol"
)

type rawRouterMock struct {
	Batches [][]byte
	mutex   sync.Mutex
	wait    *sync.WaitGroup
}

func (m *rawRouterMock) GetBatches() [][]byte {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.Batches
}

func (m *rawRouterMock) SendAll(batch *proto.MessageGroupBatch, attributes ...string) error {
	return nil
}

func (m *rawRouterMock) SendRawAll(data []byte, attributes ...string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	dst := make([]byte, len(data))
	copy(dst, data)
	m.Batches = append(m.Batches, dst)
	m.wait.Done()
	return nil
}

func (m *rawRouterMock) SubscribeAll(listener mq.Listener, attributes ...string) (queue.Monitor, error) {
	return nil, nil
}

func (m *rawRouterMock) SubscribeRawAll(listener mq.RawListener, attributes ...string) (queue.Monitor, error) {
	return nil, nil
}

func (m *rawRouterMock) SubscribeAllWithManualAck(listener mq.ConformationListener, attributes ...string) (queue.Monitor, error) {
	return nil, nil
}

func (m *rawRouterMock) Close() error {
	return nil
}

func (m *rawRouterMock) Wait() {
	m.wait.Wait()
}

func newRawRouterMock(invocations int) *rawRouterMock {
	m := &rawRouterMock{
		Batches: make([][]byte, 0),
		wait:    &sync.WaitGroup{},
	}
	m.wait.Add(invocations)
	return m
}

func TestOneMessageSerialization(t *testing.T) {
	router := newRawRouterMock(1)
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           book,
			FlushMillis:    10000,
			BatchSizeBytes: 127,
		},
		Protocol: protocol,
		Group:    group,
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}
	data := []byte("data")
	args := batcher.MessageArguments{
		Alias:     alias,
		Direction: batcher.InDirection,
	}
	if err := b.Send(data, args); err != nil {
		t.Fatal("cannot write data", err)
	}

	router.Wait()

	batches := router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	decoder := transport.NewDecoder(batch)
	msg, groupId, err := decoder.NextMessage()
	if err != nil {
		t.Fatal("decoding message failure", err)
	}
	assert.Equal(t, 0, groupId)
	checkMsg(t, msg, args.Alias, args.Direction, args.Metadata, nil, protocol, data)

	b.Close()
}

func TestOneMessageSerializationAfterClosingBatcher(t *testing.T) {
	router := newRawRouterMock(1)
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           book,
			FlushMillis:    10000,
			BatchSizeBytes: 10000,
		},
		Protocol: protocol,
		Group:    group,
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}
	data := []byte("data")
	args := batcher.MessageArguments{
		Alias:     alias,
		Direction: batcher.InDirection,
	}
	if err := b.Send(data, args); err != nil {
		t.Fatal("cannot write data", err)
	}

	b.Close()

	router.Wait()

	batches := router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	decoder := transport.NewDecoder(batch)
	msg, groupId, err := decoder.NextMessage()
	if err != nil {
		t.Fatal("decoding message failure", err)
	}
	assert.Equal(t, 0, groupId)
	checkMsg(t, msg, args.Alias, args.Direction, args.Metadata, nil, protocol, data)
}

func TestOneMessageSerializationAfterTimeout(t *testing.T) {
	router := newRawRouterMock(1)
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           book,
			FlushMillis:    100,
			BatchSizeBytes: 10000,
		},
		Protocol: protocol,
		Group:    group,
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}

	data := []byte("data")
	args := batcher.MessageArguments{
		Alias:     alias,
		Direction: batcher.InDirection,
	}
	if err := b.Send(data, args); err != nil {
		t.Fatal("cannot write data", err)
	}

	batches := router.GetBatches()
	assert.Equal(t, 0, len(batches), "unexpected batches count")

	router.Wait()

	batches = router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	decoder := transport.NewDecoder(batch)
	msg, groupId, err := decoder.NextMessage()
	if err != nil {
		t.Fatal("decoding message failure", err)
	}
	assert.Equal(t, 0, groupId)
	checkMsg(t, msg, args.Alias, args.Direction, args.Metadata, nil, protocol, data)

	b.Close()
}

func TestOneMessageWithMetadataSerialization(t *testing.T) {
	router := newRawRouterMock(1)
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           book,
			FlushMillis:    10000,
			BatchSizeBytes: 160,
		},
		Protocol: protocol,
		Group:    group,
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}
	metadata := map[string]string{"test-property": "test-value"}
	data := []byte("data")
	args := batcher.MessageArguments{
		Alias:     alias,
		Direction: batcher.InDirection,
		Metadata:  metadata,
	}
	if err := b.Send(data, args); err != nil {
		t.Fatal("cannot write data", err)
	}

	router.Wait()

	batches := router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	decoder := transport.NewDecoder(batch)
	msg, groupId, err := decoder.NextMessage()
	if err != nil {
		t.Fatal("decoding message failure", err)
	}
	assert.Equal(t, 0, groupId)
	checkMsg(t, msg, args.Alias, args.Direction, args.Metadata, nil, protocol, data)

	b.Close()
}

func TestOneMessageWithProtocolSerialization(t *testing.T) {
	router := newRawRouterMock(1)
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           book,
			FlushMillis:    10000,
			BatchSizeBytes: 124,
		},
		Protocol: protocol,
		Group:    group,
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}
	data := []byte("data")
	args := batcher.MessageArguments{
		Alias:     alias,
		Direction: batcher.InDirection,
		Protocol:  "testA",
	}
	if err := b.Send(data, args); err != nil {
		t.Fatal("cannot write data", err)
	}

	router.Wait()

	batches := router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	decoder := transport.NewDecoder(batch)
	msg, groupId, err := decoder.NextMessage()
	if err != nil {
		t.Fatal("decoding message failure", err)
	}
	assert.Equal(t, 0, groupId)
	checkMsg(t, msg, args.Alias, args.Direction, args.Metadata, nil, args.Protocol, data)

	b.Close()
}

func checkMsg(t *testing.T, msg any, alias string, direction byte, metadata map[string]string, eventId *transport.EventID, protocol string, body []byte) {
	if assert.IsType(t, &transport.RawMessage{}, msg) {
		rawMsg := msg.(*transport.RawMessage)

		id := rawMsg.MessageId
		assert.Equal(t, alias, id.SessionAlias)
		assert.Equal(t, direction, id.Direction)
		assert.Less(t, int64(0), id.Sequence)
		assert.Nil(t, id.Subsequence)
		assert.NotNil(t, id.Timestamp)

		assert.Equal(t, metadata, rawMsg.Metadata)
		assert.Equal(t, eventId, rawMsg.EventID)
		assert.Equal(t, protocol, rawMsg.Protocol)

		assert.Equal(t, body, rawMsg.Body)
	}
}
