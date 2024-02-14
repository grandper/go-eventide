package event_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/grandper/go-serializer/serializer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

// somethingMeasured is an event with exported fields, which is what the JSON
// and the gob codecs ask for.
type somethingMeasured struct {
	Value int
	At    time.Time
}

func (e somethingMeasured) OccurredOn() time.Time { return e.At }

// indentedJSONCodec is a codec of the caller: it has the three methods a
// go-serializer asks of a codec.
type indentedJSONCodec struct{}

func (indentedJSONCodec) Encode(v any) ([]byte, error)    { return json.MarshalIndent(v, "", "  ") }
func (indentedJSONCodec) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }
func (indentedJSONCodec) EncodesJSON() bool               { return true }

// notAnEvent is a type that does not implement event.Event.
type notAnEvent struct {
	Value int
}

func TestDeserialize(t *testing.T) {
	measured := somethingMeasured{Value: 42, At: time.Date(2024, time.June, 12, 1, 10, 20, 0, time.UTC)}
	recorded := fixture.NewSomethingRecorded("a reading")

	for _, tc := range []struct {
		codec string
		// registered knows the type of the event, unregistered does not.
		registered, unregistered event.Serializer
		e                        event.Event
	}{
		{
			codec:        "the JSON codec",
			registered:   serializer.NewJSONSerializer(serializer.Register(somethingMeasured{})),
			unregistered: serializer.NewJSONSerializer(),
			e:            measured,
		},
		{
			codec:        "the gob codec",
			registered:   serializer.NewByteSerializer(serializer.Register(somethingMeasured{})),
			unregistered: serializer.NewByteSerializer(),
			e:            measured,
		},
		{
			codec:        "the Protocol Buffers codec",
			registered:   serializer.NewProtoSerializer(serializer.Register(&fixture.SomethingRecorded{})),
			unregistered: serializer.NewProtoSerializer(),
			e:            recorded,
		},
		{
			codec:        "a codec of the caller",
			registered:   serializer.NewSerializer(indentedJSONCodec{}, serializer.Register(somethingMeasured{})),
			unregistered: serializer.NewSerializer(indentedJSONCodec{}),
			e:            measured,
		},
	} {
		t.Run("should round-trip an event with "+tc.codec, func(t *testing.T) {
			data, err := tc.registered.Serialize(tc.e)
			require.NoError(t, err)

			deserialized, err := event.Deserialize(tc.registered, data)
			require.NoError(t, err)
			assertSameEvent(t, tc.e, deserialized)
		})

		t.Run("should fail to deserialize an unregistered type with "+tc.codec, func(t *testing.T) {
			data, err := tc.registered.Serialize(tc.e)
			require.NoError(t, err)

			deserialized, err := event.Deserialize(tc.unregistered, data)
			require.ErrorIs(t, err, event.ErrTypeNotRegistered)
			require.ErrorIs(t, err, serializer.ErrFailedToDeserialize, "the cause is kept")
			assert.Nil(t, deserialized)
		})

		t.Run("should tell a corrupted event from an unregistered type with "+tc.codec, func(t *testing.T) {
			deserialized, err := event.Deserialize(tc.registered, []byte("corrupted"))
			require.Error(t, err)
			require.NotErrorIs(t, err, event.ErrTypeNotRegistered)
			assert.Nil(t, deserialized)
		})
	}

	t.Run("should fail to deserialize what is not an event", func(t *testing.T) {
		publishing := serializer.NewJSONSerializer(serializer.RegisterAs("SomethingMeasured", somethingMeasured{}))
		data, err := publishing.Serialize(measured)
		require.NoError(t, err)

		receiving := serializer.NewJSONSerializer(serializer.RegisterAs("SomethingMeasured", notAnEvent{}))
		deserialized, err := event.Deserialize(receiving, data)
		require.ErrorIs(t, err, event.ErrNotAnEvent)
		assert.Nil(t, deserialized)
	})

	t.Run("should fail when an event was written with another codec", func(t *testing.T) {
		registered := serializer.Register(somethingMeasured{})
		data, err := serializer.NewByteSerializer(registered).Serialize(measured)
		require.NoError(t, err)

		deserialized, err := event.Deserialize(serializer.NewJSONSerializer(registered), data)
		require.Error(t, err)
		require.NotErrorIs(t, err, event.ErrTypeNotRegistered, "the event must not be skipped")
		assert.Nil(t, deserialized)
	})
}

func TestDeserializeWithAnotherSerializer(t *testing.T) {
	somethingHappened := fixture.NewSomethingHappened()
	data := []byte("serialized")

	t.Run("should return the event of a serializer that is not a go-serializer", func(t *testing.T) {
		own := &fixture.Serializer{Deserialized: somethingHappened}

		deserialized, err := event.Deserialize(own, data)
		require.NoError(t, err)
		assert.Same(t, somethingHappened, deserialized)
		assert.Equal(t, data, own.DeserializeInput)
	})

	t.Run("should report the type such a serializer does not know", func(t *testing.T) {
		own := &fixture.Serializer{DeserializeErr: fmt.Errorf("my serializer: %w", event.ErrTypeNotRegistered)}

		deserialized, err := event.Deserialize(own, data)
		require.ErrorIs(t, err, event.ErrTypeNotRegistered)
		assert.Nil(t, deserialized)
	})

	t.Run("should return the failure of such a serializer", func(t *testing.T) {
		deserializeErr := errors.New("failed to deserialize")
		own := &fixture.Serializer{DeserializeErr: deserializeErr}

		deserialized, err := event.Deserialize(own, data)
		require.ErrorIs(t, err, deserializeErr)
		require.NotErrorIs(t, err, event.ErrTypeNotRegistered)
		assert.Nil(t, deserialized)
	})

	t.Run("should fail when such a serializer returns what is not an event", func(t *testing.T) {
		own := &fixture.Serializer{Deserialized: notAnEvent{Value: 42}}

		deserialized, err := event.Deserialize(own, data)
		require.ErrorIs(t, err, event.ErrNotAnEvent)
		assert.Nil(t, deserialized)
	})
}

// assertSameEvent asserts that two events have the same content. Protocol
// Buffers messages are compared with proto.Equal: serializing a message
// writes to its internal state.
func assertSameEvent(t *testing.T, expected, actual event.Event) {
	t.Helper()
	expectedMessage, isMessage := expected.(proto.Message)
	if !isMessage {
		assert.Equal(t, expected, actual)
		return
	}
	actualMessage, ok := actual.(proto.Message)
	require.True(t, ok, "expected a Protocol Buffers message, got %T", actual)
	assert.True(t, proto.Equal(expectedMessage, actualMessage), "expected %v, got %v", expected, actual)
}

// A go-serializer is an event.Serializer as it is.
var _ event.Serializer = (*serializer.Serializer)(nil)
