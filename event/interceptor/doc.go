// Package interceptor provides gRPC server interceptors that open a private
// event scope for each call or stream, so handlers can publish through their
// context.
package interceptor
