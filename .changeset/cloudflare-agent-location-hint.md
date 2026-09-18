---
'@flue/runtime': minor
'@flue/vite': minor
---

Cloudflare agents can now declare a `locationHint` static. Flue forwards the hint whenever it first addresses an agent's Durable Object, including HTTP requests, dispatch, and instance lookup:

```ts
export function SupportAgent() {
  return 'Support the user.';
}
SupportAgent.locationHint = 'apac';
```

Location hints are best-effort latency preferences for newly created instances; they do not move existing instances or guarantee data residency.
