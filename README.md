# Hostkit

`hostkit` is the pure Rust foundation for a standalone native capability
service. It owns reliable system mechanisms and exposes them through a
language-neutral boundary.

The core does not embed, depend on, or optimize for a client language. Reference
clients and third-party consumers must use the same public seam.

## Scope

- `net`: pooled buffered and streaming network transport
- `sys`: cross-platform process execution and lifecycle management
- `fs`: filesystem access contained within explicitly granted roots
- `ast`: statically compiled Tree-sitter analysis and token counting
- `kv`: durable namespaced state backed by SQLite
- `secrets`: controlled credential access and redaction
- `runtime`: request routing, task ownership, cancellation, and errors

Rust owns mechanism, resource safety, and platform behavior. Provider payloads,
agent behavior, prompts, tools, sessions, extensions, and other product policy
belong outside the core.

## Boundary

Every consumer receives the same capability contract. Client-specific values,
exceptions, schedulers, and lifecycle assumptions must not leak into core types
or behavior. Long-lived native resources belong to a request context and are
represented outside the core by opaque handles.

Transport and serialization remain intentionally unspecified until their
requirements are implemented and tested. The current crate is a skeleton for
the library that will back the standalone binary.

The finished binary should be deterministic, cross-platform, independently
testable, and free from runtime native-library discovery. Required native assets,
including the supported Tree-sitter grammars, should be compiled into it.
