package event

import "errors"

var (
	// ErrNoPublisher is returned when subscribing through a context that
	// holds no publisher.
	ErrNoPublisher = errors.New("the context holds no publisher")

	// ErrTypeNotRegistered is returned by Deserialize when the serializer
	// does not know the type of the event. Those who read the events of
	// other services skip such an event: a service registers the events it is
	// interested in, not all the events it may receive. A Serializer that is
	// not a go-serializer reports a type it does not know with an error
	// matching it.
	ErrTypeNotRegistered = errors.New("the type of the event is not registered")

	// ErrNotAnEvent is returned by Deserialize when what the serializer
	// deserialized does not implement Event.
	ErrNotAnEvent = errors.New("the type is not an event")
)
