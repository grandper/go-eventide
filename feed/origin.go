package feed

import "time"

// OriginKind is the kind of Origin.
type OriginKind int

const (
	// OriginStart is the kind of FromStart.
	OriginStart OriginKind = iota
	// OriginEnd is the kind of FromEnd.
	OriginEnd
	// OriginTime is the kind of Since.
	OriginTime
	// OriginPosition is the kind of After.
	OriginPosition
)

// Origin tells where a reading of the feed begins. Its zero value is
// FromStart().
type Origin struct {
	kind     OriginKind
	position int64
	time     time.Time
}

// FromStart begins before the first event of the feed.
func FromStart() Origin {
	return Origin{kind: OriginStart}
}

// FromEnd begins after the last event stored so far: only the events stored
// from now on are read.
func FromEnd() Origin {
	return Origin{kind: OriginEnd}
}

// Since begins with the first event of the feed that occurred at or after t.
// Events that occurred before t can follow it: an event stored late comes after
// events that occurred after it.
func Since(t time.Time) Origin {
	return Origin{kind: OriginTime, time: t}
}

// After begins after the event of the given id. After(0) is FromStart().
func After(position int64) Origin {
	if position == 0 {
		return FromStart()
	}
	return Origin{kind: OriginPosition, position: position}
}

// Kind returns the kind of the origin.
func (o Origin) Kind() OriginKind {
	return o.kind
}

// Position returns the position of an origin of kind OriginPosition, and 0
// for the other kinds.
func (o Origin) Position() int64 {
	return o.position
}

// Time returns the time of an origin of kind OriginTime, and the zero time for
// the other kinds.
func (o Origin) Time() time.Time {
	return o.time
}
