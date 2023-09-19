```
Need to define env vars:

envAsyncMaxMem - int
envAsyncMaxTime	- int, seconds (typically 1-3)
envAsyncThreads	- int
envAsyncWait - true/false
envBulkAgg - int, seconds for client to aggregate traffic (typically 5-10)
envDbHost - str
envDbPass - str
envDbPort - int (typically 9440 in CH cloud)
envDbSecure - true/false (typically true in CH cloud)
envDbUser - str
envDedup - true/false (typically false for current tests)
envDur - str duration in go format (e.g. 5h)
envInputDir - str, absolute path to directory with input.json.gz
envNullTable - true/false (typically false)
envQueryTop - true/false. run select query (typically one instance = true, others [if exist] = false)
envReadSec - int, seconds. duration to query (relevant if envQueryTop is true, typically 3600)
envTenantRate - int, rows/sec per tenant (typically 30000)
envTenants - int (typically 10 per instance)

> go test -count 1 -timeout 0 -v -run TestClickManualAsyncCompr
```