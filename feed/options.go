package feed

import "log/slog"

// Option is an option of those who follow a feed: NewConsumer and NewRelay.
type Option func(*settings)

// settings are what the options set.
type settings struct {
	origin Origin
	limit  int
	logger *slog.Logger
}

// StartingFrom sets where the reading begins when no position is saved yet.
// The default is FromStart().
func StartingFrom(origin Origin) Option {
	return func(s *settings) {
		s.origin = origin
	}
}

// WithBatchLimit sets the number of events read at once. The default is
// DefaultLimit.
func WithBatchLimit(limit int) Option {
	return func(s *settings) {
		s.limit = limit
	}
}

// WithLogger sets the logger of the failed runs of Follow. The default is
// slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(s *settings) {
		s.logger = logger
	}
}

func newSettings(options []Option) settings {
	s := settings{
		origin: FromStart(),
		limit:  DefaultLimit,
		logger: slog.Default(),
	}
	for _, option := range options {
		option(&s)
	}
	return s
}
