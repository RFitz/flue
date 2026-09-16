---
'@flue/runtime': minor
---

Cloudflare Sandbox commands can now report stdout and stderr while they run. Pass an `onOutput(stream, chunk)` callback to `harness.sandbox.exec()` to receive each piece of output as it arrives. The command still returns its complete buffered `stdout`, `stderr`, and exit code when it finishes.

Callback failures do not fail the command. Configure `cloudflareSandbox(..., { onObserverError })` to send those failures to your logger; without that hook, Flue reports them to stderr. Sandbox adapter authors can support the same streaming contract by forwarding `SandboxDriver.exec()`'s new `onOutput` option.
