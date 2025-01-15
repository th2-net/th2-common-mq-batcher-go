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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	proto "github.com/th2-net/th2-common-go/pkg/common/grpc/th2_grpc_common"
	"github.com/th2-net/th2-common-go/pkg/queue"
	mq "github.com/th2-net/th2-common-go/pkg/queue/message"
	"github.com/th2-net/th2-common-mq-batcher-go/pkg/batcher"
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
	initialSequence := time.Now().UnixNano()
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           "book",
			FlushMillis:    10000,
			BatchSizeBytes: 123,
		},
		Protocol: "test",
		Group:    "group",
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}
	_, err = b.Send([]byte("data"), batcher.MessageArguments{
		Alias:     "alias",
		Direction: batcher.IN,
	})
	if err != nil {
		t.Fatal("cannot write data", err)
	}

	router.Wait()

	batches := router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	assert.Equal(t, 123, len(batch))

	index := 0
	// batch
	assert.Equal(t, uint8(50), batch[index], "unexpected batch type")
	assert.Equal(t, 118, extractLen(batch[index+1:]), "unexpected batch length")
	index += 5

	// book
	index += checkString(t, 101, "book", batch, index)

	// group
	index += checkString(t, 102, "group", batch, index)

	// group list
	assert.Equal(t, uint8(51), batch[index], "unexpected group list type")
	assert.Equal(t, 94, extractLen(batch[index+1:]), "unexpected group list length")
	index += 5

	// message group
	assert.Equal(t, uint8(40), batch[index], "unexpected message group type")
	assert.Equal(t, 89, extractLen(batch[index+1:]), "unexpected message group length")
	index += 5

	// message list
	assert.Equal(t, uint8(41), batch[index], "unexpected message list type")
	assert.Equal(t, 84, extractLen(batch[index+1:]), "unexpected message list length")
	index += 5

	// message
	assert.Equal(t, uint8(20), batch[index], "unexpected message type")
	assert.Equal(t, 79, extractLen(batch[index+1:]), "unexpected message length")
	index += 5

	// msg id
	index += checkTypeAndLen(t, 10, 51, batch, index)

	// session alias
	index += checkString(t, 103, "alias", batch, index)

	// direction
	index += checkByte(t, 104, 1, batch, index)

	// sequence
	index += checkSequence(t, 105, initialSequence, batch, index)

	// subsequence
	index += checkTypeAndLen(t, 106, 0, batch, index)

	// timestamp
	index += checkTypeAndLen(t, 107, 12, batch, index)
	assert.NotEqual(t, uint64(0), binary.LittleEndian.Uint64(batch[index:]), "seconds is zero")
	index += 8
	// skip nanos
	index += 4

	// metadata
	index += checkTypeAndLen(t, 11, 0, batch, index)

	// protocol
	index += checkString(t, 12, "test", batch, index)

	// body
	index += checkString(t, 21, "data", batch, index)

	assert.Equal(t, len(batch), index)

	b.Close()
}

func TestOneMessageSerializationAfterClosingBatcher(t *testing.T) {
	router := newRawRouterMock(1)
	initialSequence := time.Now().UnixNano()
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           "book",
			FlushMillis:    10000,
			BatchSizeBytes: 10000,
		},
		Protocol: "test",
		Group:    "group",
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}
	_, err = b.Send([]byte("data"), batcher.MessageArguments{
		Alias:     "alias",
		Direction: batcher.IN,
	})
	if err != nil {
		t.Fatal("cannot write data", err)
	}

	b.Close()

	router.Wait()

	batches := router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	assert.Equal(t, 123, len(batch))

	index := 0
	// batch
	assert.Equal(t, uint8(50), batch[index], "unexpected batch type")
	assert.Equal(t, 118, extractLen(batch[index+1:]), "unexpected batch length")
	index += 5

	// book
	index += checkString(t, 101, "book", batch, index)

	// group
	index += checkString(t, 102, "group", batch, index)

	// group list
	assert.Equal(t, uint8(51), batch[index], "unexpected group list type")
	assert.Equal(t, 94, extractLen(batch[index+1:]), "unexpected group list length")
	index += 5

	// message group
	assert.Equal(t, uint8(40), batch[index], "unexpected message group type")
	assert.Equal(t, 89, extractLen(batch[index+1:]), "unexpected message group length")
	index += 5

	// message list
	assert.Equal(t, uint8(41), batch[index], "unexpected message list type")
	assert.Equal(t, 84, extractLen(batch[index+1:]), "unexpected message list length")
	index += 5

	// message
	assert.Equal(t, uint8(20), batch[index], "unexpected message type")
	assert.Equal(t, 79, extractLen(batch[index+1:]), "unexpected message length")
	index += 5

	// msg id
	index += checkTypeAndLen(t, 10, 51, batch, index)

	// session alias
	index += checkString(t, 103, "alias", batch, index)

	// direction
	index += checkByte(t, 104, 1, batch, index)

	// sequence
	index += checkSequence(t, 105, initialSequence, batch, index)

	// subsequence
	index += checkTypeAndLen(t, 106, 0, batch, index)

	// timestamp
	index += checkTypeAndLen(t, 107, 12, batch, index)
	assert.NotEqual(t, uint64(0), binary.LittleEndian.Uint64(batch[index:]), "seconds is zero")
	index += 8
	// skip nanos
	index += 4

	// metadata
	index += checkTypeAndLen(t, 11, 0, batch, index)

	// protocol
	index += checkString(t, 12, "test", batch, index)

	// body
	index += checkString(t, 21, "data", batch, index)

	assert.Equal(t, len(batch), index)
}

func TestOneMessageSerializationAfterTimeout(t *testing.T) {
	router := newRawRouterMock(1)
	initialSequence := time.Now().UnixNano()
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           "book",
			FlushMillis:    100,
			BatchSizeBytes: 10000,
		},
		Protocol: "test",
		Group:    "group",
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}

	_, err = b.Send([]byte("data"), batcher.MessageArguments{
		Alias:     "alias",
		Direction: batcher.IN,
	})
	if err != nil {
		t.Fatal("cannot write data", err)
	}

	batches := router.GetBatches()
	assert.Equal(t, 0, len(batches), "unexpected batches count")

	router.Wait()

	batches = router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	assert.Equal(t, 123, len(batch))

	index := 0
	// batch
	assert.Equal(t, uint8(50), batch[index], "unexpected batch type")
	assert.Equal(t, 118, extractLen(batch[index+1:]), "unexpected batch length")
	index += 5

	// book
	index += checkString(t, 101, "book", batch, index)

	// group
	index += checkString(t, 102, "group", batch, index)

	// group list
	assert.Equal(t, uint8(51), batch[index], "unexpected group list type")
	assert.Equal(t, 94, extractLen(batch[index+1:]), "unexpected group list length")
	index += 5

	// message group
	assert.Equal(t, uint8(40), batch[index], "unexpected message group type")
	assert.Equal(t, 89, extractLen(batch[index+1:]), "unexpected message group length")
	index += 5

	// message list
	assert.Equal(t, uint8(41), batch[index], "unexpected message list type")
	assert.Equal(t, 84, extractLen(batch[index+1:]), "unexpected message list length")
	index += 5

	// message
	assert.Equal(t, uint8(20), batch[index], "unexpected message type")
	assert.Equal(t, 79, extractLen(batch[index+1:]), "unexpected message length")
	index += 5

	// msg id
	index += checkTypeAndLen(t, 10, 51, batch, index)

	// session alias
	index += checkString(t, 103, "alias", batch, index)

	// direction
	index += checkByte(t, 104, 1, batch, index)

	// sequence
	index += checkSequence(t, 105, initialSequence, batch, index)

	// subsequence
	index += checkTypeAndLen(t, 106, 0, batch, index)

	// timestamp
	index += checkTypeAndLen(t, 107, 12, batch, index)
	assert.NotEqual(t, uint64(0), binary.LittleEndian.Uint64(batch[index:]), "seconds is zero")
	index += 8
	// skip nanos
	index += 4

	// metadata
	index += checkTypeAndLen(t, 11, 0, batch, index)

	// protocol
	index += checkString(t, 12, "test", batch, index)

	// body
	index += checkString(t, 21, "data", batch, index)

	assert.Equal(t, len(batch), index)

	b.Close()

	router.Wait()
}

func TestOneMessageWithMetadataSerialization(t *testing.T) {
	router := newRawRouterMock(1)
	initialSequence := time.Now().UnixNano()
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           "book",
			FlushMillis:    10000,
			BatchSizeBytes: 156,
		},
		Protocol: "test",
		Group:    "group",
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}
	metadata := map[string]string{"test-property": "test-value"}
	_, err = b.Send([]byte("data"), batcher.MessageArguments{
		Alias:     "alias",
		Direction: batcher.IN,
		Metadata:  metadata,
	})
	if err != nil {
		t.Fatal("cannot write data", err)
	}

	router.Wait()

	batches := router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	assert.Equal(t, 156, len(batch))

	index := 0
	// batch
	assert.Equal(t, uint8(50), batch[index], "unexpected batch type")
	assert.Equal(t, 151, extractLen(batch[index+1:]), "unexpected batch length")
	index += 5

	// book
	index += checkString(t, 101, "book", batch, index)

	// group
	index += checkString(t, 102, "group", batch, index)

	// group list
	assert.Equal(t, uint8(51), batch[index], "unexpected group list type")
	assert.Equal(t, 127, extractLen(batch[index+1:]), "unexpected group list length")
	index += 5

	// message group
	assert.Equal(t, uint8(40), batch[index], "unexpected message group type")
	assert.Equal(t, 122, extractLen(batch[index+1:]), "unexpected message group length")
	index += 5

	// message list
	assert.Equal(t, uint8(41), batch[index], "unexpected message list type")
	assert.Equal(t, 117, extractLen(batch[index+1:]), "unexpected message list length")
	index += 5

	// message
	assert.Equal(t, uint8(20), batch[index], "unexpected message type")
	assert.Equal(t, 112, extractLen(batch[index+1:]), "unexpected message length")
	index += 5

	// msg id
	index += checkTypeAndLen(t, 10, 51, batch, index)

	// session alias
	index += checkString(t, 103, "alias", batch, index)

	// direction
	index += checkByte(t, 104, 1, batch, index)

	// sequence
	index += checkSequence(t, 105, initialSequence, batch, index)

	// subsequence
	index += checkTypeAndLen(t, 106, 0, batch, index)

	// timestamp
	index += checkTypeAndLen(t, 107, 12, batch, index)
	assert.NotEqual(t, uint64(0), binary.LittleEndian.Uint64(batch[index:]), "seconds is zero")
	index += 8
	// skip nanos
	index += 4

	// metadata
	index += checkMetadata(t, 11, metadata, batch, index)

	// protocol
	index += checkString(t, 12, "test", batch, index)

	// body
	index += checkString(t, 21, "data", batch, index)

	assert.Equal(t, len(batch), index)

	b.Close()
}

func TestOneMessageWithProtocolSerialization(t *testing.T) {
	router := newRawRouterMock(1)
	initialSequence := time.Now().UnixNano()
	b, err := batcher.NewMessageBatcher(router, batcher.MqMessageBatcherConfig{
		MqBatcherConfig: batcher.MqBatcherConfig{
			Book:           "book",
			FlushMillis:    10000,
			BatchSizeBytes: 124,
		},
		Protocol: "test",
		Group:    "group",
	})
	if err != nil {
		t.Fatal("cannot create writer", err)
	}
	_, err = b.Send([]byte("data"), batcher.MessageArguments{
		Alias:     "alias",
		Direction: batcher.IN,
		Protocol:  "testA",
	})
	if err != nil {
		t.Fatal("cannot write data", err)
	}

	router.Wait()

	batches := router.GetBatches()
	assert.Equal(t, 1, len(batches), "unexpected batches count")

	batch := batches[0]
	assert.Equal(t, 124, len(batch))

	index := 0
	// batch
	assert.Equal(t, uint8(50), batch[index], "unexpected batch type")
	assert.Equal(t, 119, extractLen(batch[index+1:]), "unexpected batch length")
	index += 5

	// book
	index += checkString(t, 101, "book", batch, index)

	// group
	index += checkString(t, 102, "group", batch, index)

	// group list
	assert.Equal(t, uint8(51), batch[index], "unexpected group list type")
	assert.Equal(t, 95, extractLen(batch[index+1:]), "unexpected group list length")
	index += 5

	// message group
	assert.Equal(t, uint8(40), batch[index], "unexpected message group type")
	assert.Equal(t, 90, extractLen(batch[index+1:]), "unexpected message group length")
	index += 5

	// message list
	assert.Equal(t, uint8(41), batch[index], "unexpected message list type")
	assert.Equal(t, 85, extractLen(batch[index+1:]), "unexpected message list length")
	index += 5

	// message
	assert.Equal(t, uint8(20), batch[index], "unexpected message type")
	assert.Equal(t, 80, extractLen(batch[index+1:]), "unexpected message length")
	index += 5

	// msg id
	index += checkTypeAndLen(t, 10, 51, batch, index)

	// session alias
	index += checkString(t, 103, "alias", batch, index)

	// direction
	index += checkByte(t, 104, 1, batch, index)

	// sequence
	index += checkSequence(t, 105, initialSequence, batch, index)

	// subsequence
	index += checkTypeAndLen(t, 106, 0, batch, index)

	// timestamp
	index += checkTypeAndLen(t, 107, 12, batch, index)
	assert.NotEqual(t, uint64(0), binary.LittleEndian.Uint64(batch[index:]), "seconds is zero")
	index += 8
	// skip nanos
	index += 4

	// metadata
	index += checkMetadata(t, 11, nil, batch, index)

	// protocol
	index += checkString(t, 12, "testA", batch, index)

	// body
	index += checkString(t, 21, "data", batch, index)

	assert.Equal(t, len(batch), index)

	b.Close()
}

func checkSequence(t *testing.T, codec uint8, value int64, src []byte, startIndex int) int {
	startIndex += checkTypeAndLen(t, codec, 8, src, startIndex)
	assert.Less(t, value, int64(binary.LittleEndian.Uint64(src[startIndex:])), "unexpected value")
	return 5 + 8
}

func checkMetadata(t *testing.T, codec uint8, value map[string]string, src []byte, startIndex int) int {
	length := 0
	for k, v := range value {
		// type: 1, length: 4, value: len(<...>)
		length += len(k) + 5
		length += len(v) + 5
	}

	startIndex += checkTypeAndLen(t, codec, length, src, startIndex)
	for k, v := range value {
		startIndex += checkString(t, 2, k, src, startIndex)
		startIndex += checkString(t, 2, v, src, startIndex)
	}

	return 5 + length
}

func checkString(t *testing.T, codec uint8, value string, src []byte, startIndex int) int {
	startIndex += checkTypeAndLen(t, codec, len(value), src, startIndex)
	assert.Equal(t, value, string(src[startIndex:startIndex+len(value)]), "unexpected value")
	return 5 + len(value)
}

func checkByte(t *testing.T, codec uint8, value byte, src []byte, startIndex int) int {
	startIndex += checkTypeAndLen(t, codec, 1, src, startIndex)
	assert.Equal(t, value, src[startIndex], "unexpected value")
	return 5 + 1
}

func checkLong(t *testing.T, codec uint8, value int64, src []byte, startIndex int) int {
	startIndex += checkTypeAndLen(t, codec, 8, src, startIndex)
	assert.Equal(t, value, int64(binary.LittleEndian.Uint64(src[startIndex:])), "unexpected value")
	return 5 + 8
}

func checkTypeAndLen(t *testing.T, codec uint8, len int, src []byte, startIndex int) int {
	assert.Equal(t, codec, src[startIndex], "unexpected type")
	assert.Equal(t, len, extractLen(src[startIndex+1:]), "unexpected length")
	return 5
}

func extractLen(src []byte) int {
	return int(binary.LittleEndian.Uint32(src))
}
