package fixture

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/grandper/go-eventide/event"
)

//go:generate protoc --go_out=. --go_opt=paths=source_relative something_recorded.proto

// NewSomethingRecorded creates a new SomethingRecorded event, a sample event
// that is a Protocol Buffers message.
func NewSomethingRecorded(what string) *SomethingRecorded {
	return &SomethingRecorded{
		What:       what,
		OccurredAt: timestamppb.New(time.Date(2024, time.June, 12, 1, 10, 20, 0, time.UTC)),
	}
}

// OccurredOn returns when the event occurred.
func (x *SomethingRecorded) OccurredOn() time.Time {
	return x.GetOccurredAt().AsTime()
}

// SomethingRecorded implements the event.Event interface.
var _ event.Event = &SomethingRecorded{}
