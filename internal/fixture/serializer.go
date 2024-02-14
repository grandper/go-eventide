package fixture

import "github.com/grandper/go-eventide/event"

// Serializer is a test double that returns preset values and records its
// inputs. Serialize returns Serialized/SerializeErr; Deserialize returns
// Deserialized/DeserializeErr.
type Serializer struct {
	// Serialized is returned by Serialize.
	Serialized []byte
	// SerializeErr is returned by Serialize.
	SerializeErr error
	// SerializeInput captures the last value passed to Serialize.
	SerializeInput any

	// Deserialized is returned by Deserialize.
	Deserialized any
	// DeserializeErr is returned by Deserialize.
	DeserializeErr error
	// DeserializeInput captures the last payload passed to Deserialize.
	DeserializeInput []byte
}

// Serialize records the event and returns the preset result.
func (s *Serializer) Serialize(v any) ([]byte, error) {
	s.SerializeInput = v
	return s.Serialized, s.SerializeErr
}

// Deserialize records the bytes and returns the preset result.
func (s *Serializer) Deserialize(data []byte) (any, error) {
	s.DeserializeInput = data
	return s.Deserialized, s.DeserializeErr
}

// Serializer implements the event.Serializer interface.
var _ event.Serializer = &Serializer{}
