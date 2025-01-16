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

import "time"

type Direction = byte

const (
	UNKNOWN Direction = iota
	IN
	OUT
)

type MqMessageBatcherConfig struct {
	MqBatcherConfig
	Group    string
	Protocol string
}

type stream struct {
	alias     string
	direction Direction
}

type sequenceProvider map[stream]int64

func (p sequenceProvider) nextSeq(alias string, direction Direction) int64 {
	key := stream{alias, direction}
	value, exist := p[key]
	if exist {
		value += 1
	} else {
		value = time.Now().UnixNano()
	}
	p[key] = value
	return value
}
