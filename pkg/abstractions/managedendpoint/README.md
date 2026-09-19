# Managed endpoint usage and pricing

Managed endpoints publish prices in the hosted repository's `config.yaml`.
`llm`, `embedding`, and `custom` endpoints may use token pricing. Custom engines
keep `kind="custom"` and their model-scoped `/invoke` route.

```yaml
acme/decision:
  enabled: true
  pricing:
    prompt_tokens: "0.000000021" # USD per input token ($0.021 per million)
    completion_tokens: "0"     # Free output
  gpus: {RTX5090: {priority: 1, serverless: true, maxReplicas: 1}}
```

A JSON response may report OpenAI `usage.prompt_tokens` and
`usage.completion_tokens`, or TypeSafe-style `usage.input_tokens` and
`usage.output_tokens`. The latter must include both counters, including explicit
zero. Counters must be nonnegative integers within the platform's exact counter
limit. Missing, null, malformed, or mixed counter formats do not represent
measured work: a token-priced success becomes an unbilled `502 missing_usage`.
OpenAI embeddings may omit `completion_tokens`, which counts as zero.

Input/output counters map to the existing prompt/completion billing, analytics,
and usage meters. The response keeps its original counter names. OpenAI cached
input remains supported through `prompt_tokens_details.cached_tokens`; the
input/output format does not report cache discounts.

Flat `pricing: {request: "0.002"}` still bills one successful request and ignores
token counters. Request and token pricing remain mutually exclusive. Failed
requests are never billed, and retries of a journaled charge are metered once.

Prices are decimal USD per token. The existing ledger rounds each request's
total half up to whole micro-USD, then reconciles its cost components. At the
example rate, 23 input tokens round to zero micro-USD and 24 round to one; there
is no accumulation of fractions across requests. Published rates do not change
this ledger precision.

`beta9 endpoints validate` sends a dry run to the selected gateway; installing
an updated local SDK cannot add support to an older gateway. Custom token
pricing needs a gateway containing this support before it can be enabled.
The SDK and protobuf schemas do not need new pricing fields.
