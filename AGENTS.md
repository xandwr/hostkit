# tau-core Development Rules

Read `README.md` before making architectural changes.

- Keep the Rust core independent of every client language and runtime.
- Do not add embedded language runtimes or client-specific types, errors, or scheduling semantics.
- Keep provider, agent, prompt, tool, session, extension, and interface policy outside this repository.
- Expose native behavior only through the shared language-neutral boundary.
- Grant capabilities explicitly and contain resources within their owning request context.
- Preserve cancellation, cleanup, limits, and structured errors across every capability.
- Prefer statically compiled native dependencies and assets over runtime discovery.
- Treat Windows, Linux, and macOS behavior as one supported contract.
- Keep capability modules isolated and independently testable.
- Do not design speculative APIs. Add contracts alongside the implementation and tests that require them.
- Run `cargo fmt --check`, `cargo check`, `cargo test`, and `cargo doc --no-deps` after changes.
