# Hostkit

`hostkit` is a pure Rust foundation for a standalone native capability service.
it owns reliable system mechanisms and exposes them through a language-neutral boundary.

core doesn't embed, depend on, or optimize for a client language.
reference clients and third-party consumers use the same API.

## Potential use cases:

- Sandboxed capability hosting for desktop application plugins.
- Cross-platform build, test, and automation workers.
- Native sidecars for applications written in managed or scripting languages.
- Secure execution daemons for cancellable, resource-contained jobs.
- LLM agent infrastructure.

## Scope:

- `net`: pooled buffered and streaming network transport.
- `sys`: cross-platform process execution and lifecycle management.
- `fs`: filesystem access contained within explicitly granted roots.
- `ast`: statically compiled Tree-sitter analysis and token counting.
- `kv`: durable namespaced state backed by SQLite.
- `secrets`: controlled credential access and redaction.
- `runtime`: request routing, task ownership, cancellation, and errors.

Rust owns mechanism, resource safety, and platform behavior.
provider payloads, agent behavior, prompts, tools, sessions, extensions,
and other bullshit belong outside the core.

## Boundaries:

every consumer receives the same capability contract.
client-specific values, exceptions, schedulers, and assumptions can not leak
into core types or behavior. long-lived native resources belong to a request
context and are represented outside the core by opaque handles.

oh, and transport and serialization remain intentionally unspecified until their
requirements are implemented and tested.

the binary is deterministic, cross-platform, independently testable,
and free from runtime native-library discovery.
required native assets, including the supported Tree-sitter grammars, are compiled into it.
