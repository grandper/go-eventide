package feed

// Batch is a run of consecutive events of the feed.
type Batch struct {
	events  []*StoredEvent
	next    int64
	hasMore bool
}

// NewBatch creates a new batch. next is the position to read the following
// batch from: the id of the last event of the batch, or the position the
// reading began at when the batch is empty.
func NewBatch(events []*StoredEvent, next int64, hasMore bool) *Batch {
	if events == nil {
		events = []*StoredEvent{}
	}
	return &Batch{
		events:  events,
		next:    next,
		hasMore: hasMore,
	}
}

// Events returns the events of the batch, in ascending id order. It is never
// nil.
func (b *Batch) Events() []*StoredEvent {
	return b.events
}

// Next returns the position to read the following batch from.
func (b *Batch) Next() int64 {
	return b.next
}

// HasMore returns true when events exist after Next().
func (b *Batch) HasMore() bool {
	return b.hasMore
}
