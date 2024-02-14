// Package eventtest provides helpers to test code publishing events: a Checker
// recording the events it receives so they can be asserted on, and scopes
// returning such a checker.
//
// It depends on the testing package and is meant to be imported from tests
// only; the event package itself stays free of test-only imports.
package eventtest
