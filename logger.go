package asg3

import (
	"fmt"
	"log"
)

// =================================
//  Event logger, internal use only
// =================================

type Logger struct {
	// index = time step
	// value = events that occurred at that time step
	events [][]LogEvent
}

type LogEvent struct {
	nodeId string
	// Number of tokens before execution of event
	nodeTokens int
	record     interface{}
}

func (event LogEvent) String() string {
	prependTokens := false
	switch event := event.record.(type) {
	case SentMsgRecord:
		if !event.message.isMarker {
			prependTokens = true
		}
	case ReceivedMsgRecord:
		if !event.message.isMarker {
			prependTokens = true
		}
	case StartSnapshotRecord:
		prependTokens = true
	case EndSnapshotRecord:
	default:
		log.Fatal("Attempted to log unrecognized event: ", event)
	}
	if prependTokens {
		return fmt.Sprintf("%v has %v token(s)\n\t%v",
			event.nodeId,
			event.nodeTokens,
			event.record)
	} else {
		return fmt.Sprintf("%v", event.record)
	}
}

func NewLogger() *Logger {
	return &Logger{make([][]LogEvent, 0)}
}

func (log *Logger) PrettyPrint() {
	for epoch, events := range log.events {
		if len(events) != 0 {
			fmt.Printf("Time %v:\n", epoch)
		}
		for _, event := range events {
			fmt.Printf("\t%v\n", event)
		}
	}
}

func (log *Logger) NewEpoch() {
	log.events = append(log.events, make([]LogEvent, 0))
}

func (logger *Logger) RecordEvent(node *Node, eventRecord interface{}) {
	mostRecent := len(logger.events) - 1
	events := logger.events[mostRecent]
	events = append(events, LogEvent{node.id, node.tokens, eventRecord})
	logger.events[mostRecent] = events
}
