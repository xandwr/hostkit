# Hostkit Development Rules

Read `README.md` before making architectural changes.

## Git workflow

- Keep `main` as the canonical integration branch.
- Create a `feat/<slug>` branch for new functionality.
- Create a `change/<slug>` branch for intentional changes to existing behavior or contracts.
- Create a `fix/<slug>` branch for corrections to unintended behavior.
- Validate feature, change, and fix branches, then squash-merge them into `main` with a matching `feat:`, `change:`, or `fix:` commit subject.
- Nonfunctional cleanup and organization may be committed directly to `main` with a `chore:` subject.
- Do not mix functional changes into a chore. When classification is uncertain, use a `change/<slug>` branch.
- Agents may push validated commits and completed merges to `origin` without separate approval when this workflow is followed.
- Never force-push or otherwise rewrite remote `main`. Correct mistakes with explicit follow-up or revert commits.
- Delete a topic branch after it is merged.

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
- Use `.scratch/` for disposable fixtures, experiments, generated artifacts, and local test state. Its contents are ignored and must never become an implementation dependency.
- Run `cargo fmt --check`, `cargo check`, `cargo test`, and `cargo doc --no-deps` after changes.
