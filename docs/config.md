# Configuration

For basic configuration instructions, see [this documentation](https://developers.openai.com/codex/config-basic).

For advanced configuration instructions, see [this documentation](https://developers.openai.com/codex/config-advanced).

For a full configuration reference, see [this documentation](https://developers.openai.com/codex/config-reference).

## Idle compaction

Idle compaction is enabled by default. It attempts to compact a completed or
failed root thread before an estimated prompt-cache window closes. To disable it:

```toml
[features]
idle_compact = false
```

It can also be disabled with `codex features disable idle_compact`. Feature
changes apply to new sessions; they do not change an already running session.

Codex derives timing estimates for recognized OpenAI models from the [OpenAI API prompt-cache policies](https://developers.openai.com/api/docs/guides/prompt-caching#summary-of-model-differences): 30 minutes for recognized GPT-5.5, GPT-5.6, GPT-6, GPT-6.1, and GPT-6.2 families, and a conservative five minutes for recognized earlier families. For unknown providers or models, Codex assumes a 30-minute window. These are scheduling estimates, not guaranteed cache hits; retention can depend on the provider, endpoint, account policy, and request settings. By default, the attempt starts five sixths of the way through the estimated window. If the thread is busy, Codex checks again after half the remaining interval (at least one second); it does not schedule a second idle compaction after one has started.

A feature table can supply global, provider, or model-specific overrides; more specific values take precedence:

```toml
[features.idle_compact]
enabled = true

[features.idle_compact.providers.my_provider]
cache_window_seconds = 600

[features.idle_compact.providers.my_provider.models."my-model"]
cache_window_seconds = 1800
delay_seconds = 1500
retry_seconds = 120
```

Each timing field may also be set directly under `[features.idle_compact]`.
The delay must be shorter than the effective window, and the retry interval and
window must be positive. If an explicit delay does not fit a model's inferred
window, that model is skipped. Compaction may itself use a model request.
New user input cancels a pending attempt or interrupts an idle compaction that
has started. Codex does not initiate an idle-compaction turn at or after the
estimated window; preparation and hooks can delay its model request.
Threads already compacted at the end of their last turn and token-budget
sessions are not scheduled for idle compaction.

The timer lives in the app-server process. A daemon keeps scheduled threads
loaded long enough to attempt compaction even after their clients disconnect;
stopping or restarting the server discards pending timers. An embedded server
stops with its CLI process. Idle compaction does not delete subagent histories
or block messages in a stale thread.

## Lifecycle hooks

Admins can set top-level `allow_managed_hooks_only = true` in
`requirements.toml` to ignore user, project, and session hook configs while
still allowing managed hooks from requirements and managed config layers. This
setting is only supported in `requirements.toml`; putting it in `config.toml`
does not enable managed-hooks-only mode.
