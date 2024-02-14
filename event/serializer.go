package event

import (
	"errors"
	"fmt"

	"github.com/grandper/go-serializer/serializer"
)

// Serializer turns events into bytes, to store them or to carry them to
// another service, and back.
//
// A serializer of github.com/grandper/go-serializer is a Serializer: build
// one with the codec of your choice (JSON, gob, Protocol Buffers, or a codec
// of your own) and hand it over as it is.
//
//	s := serializer.NewJSONSerializer(serializer.Register(OrderPlaced{}))
//	forwarder := event.NewForwarder(messagePublisher, s)
//
// Those who read events do not call Deserialize themselves: they call the
// Deserialize function of this package, which returns an Event.
type Serializer interface {
	// Serialize serializes an event.
	Serialize(v any) ([]byte, error)

	// Deserialize deserializes what Serialize produced. It fails with an error
	// matching ErrTypeNotRegistered when the type of the event is not known
	// to the serializer.
	Deserialize(data []byte) (any, error)
}

// Deserialize turns a serialized event back into an event with the
// serializer. It fails with ErrTypeNotRegistered when the serializer does not
// know the type of the event, and with ErrNotAnEvent when what it deserialized
// does not implement Event.
//
// Those who read the events of other services skip an event whose type is not
// registered: a service registers the events it is interested in, not all the
// events it may receive.
func Deserialize(s Serializer, data []byte) (Event, error) {
	deserialized, err := s.Deserialize(data)
	if errors.Is(err, serializer.ErrTypeNotRegistered) {
		return nil, fmt.Errorf("%w: %w", ErrTypeNotRegistered, err)
	}
	if err != nil {
		return nil, err
	}
	e, ok := deserialized.(Event)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrNotAnEvent, deserialized)
	}
	return e, nil
}
