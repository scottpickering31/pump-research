# Reliability and restart verification

The reliability suite uses mock HTTP transports, fake discovery sources, a
test-owned PostgreSQL backend connection, and real local subprocess signals. It
does not call Pump.fun or DEX Screener.

| Failure | Verified behavior |
| --- | --- |
| HTTP timeout | Retries the configured attempts through the limiter, records failure metrics, then surfaces the timeout. |
| HTTP 429 | Uses the retry path and records throttling and retry evidence. |
| HTTP 500 | Retries through the same limiter and records the failed and successful attempts. |
| Bad JSON | Raises an explicit parse error and does not retry a malformed successful response. |
| Discovery disconnect | Leaves the durable opaque checkpoint unchanged and resumes from it after reconstruction. |
| PostgreSQL interruption | Terminates one test-owned backend connection, observes a loud exception, then verifies a new pre-ping connection succeeds. |
| Duplicate discovery | Preserves one token, source event, initial lifecycle event, and pending task. |
| Duplicate API response | Collapses an exact duplicate while retaining unchanged facts from a genuinely later request. |
| SIGKILL/process restart | Physically kills a real `python -m pump_research collector run` process and leaves its run unfinished. |
| Restart reconstruction | Replacement recovers the abandoned run and reconstructs tokens, DEX work, schedules, leases, and discovery checkpoint state from PostgreSQL. |
| SIGTERM | Replacement finalizes its run as cancelled with `SIGTERM` evidence and exits successfully. |

The collector command currently owns process lifecycle and state reconstruction;
continuous discovery and market-polling workers are not yet attached. This
distinction lets the physical restart test verify durability without exercising
live third-party APIs.
