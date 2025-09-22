# Visual Comparison: Current vs Optimized LLM Enrichment

## Current Flow (Per Gym)
```
[Gym Data] 
    |
    ├─→ [DB Query 1] → [LLM Task 1: Description] → [2000 tokens]
    |                                              → [DB Update 1]
    |
    ├─→ [DB Query 2] → [LLM Task 2: Amenities] → [2000 tokens]
    |                                           → [DB Update 2]
    |
    └─→ [DB Query 3] → [LLM Task 3: Misc Details] → [2000 tokens]
                                                   → [DB Update 3]

Total: 3 DB queries + 3 LLM calls + 6000 tokens + 3 DB updates
```

## Optimized Flow (Per Gym)
```
[Gym Data]
    |
    └─→ [Single DB Query] → [Combined LLM Task] → [2500 tokens]
                                               → [Single DB Update]

Total: 1 DB query + 1 LLM call + 2500 tokens + 1 DB update
```

## Batch Processing (10 Gyms)
```
Current Approach:
[Gym1] → 3 queries → 3 LLM calls
[Gym2] → 3 queries → 3 LLM calls
...
[Gym10] → 3 queries → 3 LLM calls
Total: 30 DB queries + 30 LLM calls

Optimized Approach:
[Gym1-10] → 1 batch query → 10 parallel combined LLM calls
Total: 1 DB query + 10 LLM calls
```

## Retry Scenario
```
Current: LLM Task Fails
→ Retry from scratch
→ Re-query DB
→ Re-process full prompt
→ 2000 tokens used again

Optimized: LLM Task Fails  
→ Check cache
→ If cached: instant return
→ If not: retry with exponential backoff
→ 0 tokens for cached results
```

## Key Improvements Summary

| Operation | Current | Optimized | Savings |
|-----------|---------|-----------|---------|
| **Per Gym** | | | |
| DB Queries | 3 | 1 | 66% |
| LLM API Calls | 3 | 1 | 66% |
| Token Usage | ~6000 | ~2500 | 58% |
| DB Updates | 3 | 1 | 66% |
| **Per 100 Gyms** | | | |
| DB Queries | 300 | 10-20 | 93-96% |
| Total Tokens | 600,000 | 250,000 | 58% |
| Est. Time | 30 min | 12 min | 60% |
| **On Retry** | | | |
| Tokens Used | Full | 0 (cached) | 100% |
| Time | Full | <1 sec | 99% |