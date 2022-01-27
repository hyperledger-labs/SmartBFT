// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

type EventType string

var (
	sanitizers = make(map[EventType]func(interface{}) interface{})
	decoders   = make(map[EventType]func([]byte) interface{})
)

func RegisterSanitizer(et EventType, f func(interface{}) interface{}) {
	sanitizers[et] = f
}

func RegisterDecoder(et EventType, f func([]byte) interface{}) {
	decoders[et] = f
}

type RecordedEvent struct {
	Type    EventType
	Content []byte
}

func NewRecordedEvent(eventType EventType, o interface{}) RecordedEvent {
	// First, sanitize the data
	sanitize, exists := sanitizers[eventType]
	if !exists {
		panic(fmt.Sprintf("no sanitizer registered for type %s", eventType))
	}

	raw, err := json.Marshal(sanitize(o))
	if err != nil {
		panic(fmt.Sprintf("failed marshaling: %v", err))
	}
	return RecordedEvent{
		Content: raw,
		Type:    eventType,
	}
}

func (re RecordedEvent) Decode() interface{} {
	return decoders[re.Type](re.Content)
}

func (re RecordedEvent) String() string {
	return fmt.Sprintf("%s %s", re.Type, string(re.Content))
}

func (re *RecordedEvent) FromString(s string) {
	sep := strings.Index(s, " ")
	if sep == -1 {
		panic(fmt.Sprintf("no space detected in record: %s", s))
	}

	re.Type = EventType(s[:sep])
	re.Content = []byte(s[sep+1:])
}
