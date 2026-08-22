# Collector V2 long-tail scheduling design

Status: design and quantitative model only. Current lifecycle thresholds, configured DEX ceiling and scheduler implementation remain unchanged.

## Problem statement

Epoch 2 ended with roughly 34,000 NEW, 24,000 FADING and 154 DORMANT schedules. At their current nominal cadences, NEW alone requests about 68,000 token observations/min and FADING about 12,000/min. Total requested demand exceeded 82,000/min while the configured safe budget was 5,760/min:

```text
safe requests/min = floor(240 × (1 - 0.20)) = 192
safe token observations/min = 192 × 30 = 5,760
```

The capacity adapter truthfully stretched effective intervals and protected ACTIVE. It prevented exceeding the request budget, but it could not make a perpetually growing eligible population useful. Nearly every schedule became degraded, and strict priority plus continual arrivals left FADING/DORMANT work many hours late. Increasing the limit only postpones the same asymptote.

V2 must make the amount of routine work per admitted token finite. Its steady-state universal demand should be proportional to the recent admission rate and active/candidate population, not every token ever observed.

## Separate lifecycle from coverage

Do not add `RETIRED` as a market lifecycle state. Keep the existing NEW/ACTIVE/WATCH/FADING/DORMANT/RESURRECTED evidence and thresholds. Add an operational, versioned `coverage_class` or schedule mode:

- `EARLY_UNIVERSAL`
- `MATURE_UNIVERSAL`
- `ACTIVE_PROTECTED`
- `WATCH_PROTECTED`
- `FADING_TAIL`
- `CANDIDATE_ENRICHED`
- `RETIRED_DIRECT_POLLING`
- `RESURRECTION_SCAN`

Every coverage transition records token, previous/new class, decided time, input watermark, reason, age/inactivity facts, policy digest, target/effective interval and epoch. It changes only future obligations. Retirement means no routine direct DEX poll; it never deletes identity, observations, lifecycle history or eligibility for deterministic/event-driven reactivation.

## Proposed universal age curve

Age is measured from first DEX-visible receipt, not nullable provider creation time. Initial validation uses this curve for NEW and other non-protected universal tokens:

| Age since DEX admission | Target interval | Observations per token in band | Purpose |
|---|---:|---:|---|
| 0–2 minutes | 15s | 8 | earliest price/liquidity/trade shape |
| 2–10 minutes | 30s | 16 | early trajectory and first lifecycle evidence |
| 10–60 minutes | 5m | 10 | preserve continued development without 30s chaff load |
| 1–6 hours | 30m | 10 | medium-horizon negative/survival evidence |
| 6–24 hours | 2h | 9 | day-one liveness and delayed activity |
| 1–7 days | 12h | 12 | sparse survival/resurrection evidence |
| after 7 days | no routine direct poll | fixed-budget rotation/event trigger | bounded long tail |

This is 65 routine observations per token that never becomes protected. It retains broad chaff and early-life negatives while eliminating an infinite per-token annuity. The exact curve is a proposed validation starting point, not a lifecycle-threshold change.

Overrides:

- ACTIVE and RESURRECTED target 5s while existing lifecycle evidence retains them there.
- WATCH initially retains the existing 15s target.
- A token leaving ACTIVE/WATCH gets a maximum 30-minute FADING tail at 120s (at most 15 extra observations) to capture collapse/recovery. Afterward, the age/inactivity curve can only cool it further.
- DORMANT leaves routine direct polling after its confirming observation and enters the bounded resurrection mechanism.
- A later candidate tier can restore richer/high cadence via a separate, audited eligibility decision.

## Pending DEX and retired resurrection

PENDING_DEX can also grow forever. Preserve admission semantics but use deterministic age-based retries, for example 1m, 5m, 30m, 2h and 12h up to seven days, followed by a fixed-budget pending rescan. Every attempt remains logged, and absence remains explicit.

For long-retired/DORMANT tokens, combine:

1. event-driven re-entry from newly observed DEX boost/profile/activity feeds, where coverage is explicitly best-effort;
2. a deterministic oldest-unscanned rotation capped initially at 2,000 retired tokens/day;
3. a separate fixed pending-Dex rotation capped initially at 1,000/day;
4. stratified controls by admission cohort/last lifecycle/age so the sample is useful for false-negative and delayed-resurrection estimates.

Selection is deterministic: order by last scan time, then a stable hash of token ID, UTC day and policy digest. At one million retired tokens, a full rotation takes about 500 days, but aggregate cost remains 1.39 token observations/min. No token is randomly or permanently dropped, and the growing interval is reported honestly.

## Capacity allocation and fairness

V2 keeps the current configured limit and capacity mathematics as a final safety layer, but it plans obligations using coverage classes before they become overdue.

Priority order:

1. ACTIVE and RESURRECTED;
2. first 10 minutes of universal life;
3. WATCH and approved candidate enrichment;
4. FADING collapse tail;
5. mature universal age bands;
6. retired and pending fixed-budget scans.

Within each class use earliest virtual deadline first, then stable token ID. Each lower class receives a persisted positive rate reservation when populated. A worker claims at most 30 addresses, across four workers, from shared PostgreSQL leases and the one service budget; no per-token task is created.

Strict priority alone is insufficient. The capacity decision should assign per-class observation tokens/minute, and due times should be generated from that effective rate. If demand changes, recompute future deadlines from the last completion and the new effective interval; do not first create thousands of impossible target deadlines and call their resulting age “lateness.” Target-versus-effective degradation remains recorded. Existing overdue work is rebased only by an explicit append-only recovery/coverage decision, never silently.

Fairness invariants:

- no class exceeds its persisted allocation;
- no token in a populated allocated class is selected twice before all earlier virtual deadlines in that class are serviced;
- leases expire and are recoverable;
- batch deduplication never erases membership evidence;
- scan rotation advances only after a durable attempt/outcome;
- target, effective and realized interval distributions are reported separately;
- when protected demand alone cannot fit, ACTIVE/RESURRECTED share capacity fairly and mode is CRITICAL.

## Quantitative steady-state model

Epoch 2 ran for about 35.50 hours and linked 50,983 distinct discoveries and 50,965 distinct NEW admissions to its runs: approximately 23.9/min for both. The model deliberately rounds this up to `λ = 28 admitted tokens/min`, about 17% arrival headroom. V2 status must measure the live admission rate continuously; the table uses 28/min as a stress assumption.

At that rate, the universal age buckets saturate as follows:

| Band | Steady population | Observation demand/min |
|---|---:|---:|
| 0–2m | 56 | 224 |
| 2–10m | 224 | 448 |
| 10–60m | 1,400 | 280 |
| 1–6h | 8,400 | 280 |
| 6–24h | 30,240 | 252 |
| 1–7d | 241,920 | 336 |
| total universal at full seven-day pipeline | 282,240 | 1,820 |

The full model below additionally assumes a conservative 120 ACTIVE/RESURRECTED tokens at 5s (1,440/min), 100 WATCH at 15s (400/min), every arrival consumes the maximum 15-observation FADING tail (420/min), and both fixed scan budgets (2.08/min). These assumptions intentionally double-count some protection relative to a typical population.

| Cumulative represented tokens | Universal age demand/min | Total modeled observations/min | Requests/min at occupancy 30 | % of 192 safe requests/min |
|---:|---:|---:|---:|---:|
| 10,000 | 1,229 | 3,491 | 116.4 | 60.6% |
| 50,000 | 1,497 | 3,759 | 125.3 | 65.3% |
| 100,000 | 1,567 | 3,829 | 127.6 | 66.5% |
| 500,000 | 1,820 | 4,082 | 136.1 | 70.9% |
| 1,000,000 | 1,820 | 4,082 | 136.1 | 70.9% |

At and above the seven-day pipeline size, cumulative population no longer changes routine demand. This is the bounded property missing from V1.

The request projection assumes efficient 30-address batches. At 4,082 observations/min the minimum average occupancy needed to remain within 192 requests/min is about 21.3. V2 should alarm if rolling occupancy approaches that threshold. Reserving 12 requests/min for availability/retries and about 2 shared boost-feed calls/min still projects about 150 requests/min total, below the existing 192 safe ceiling. Dedicated endpoint limits and a conservative all-DEX-service budget must both be enforced; this is not permission to raise either limit.

With the modeled non-ACTIVE demand and a 14-request/min service reserve, roughly 2,700 token observations/min remain for protected work, enough for about 225 ACTIVE/RESURRECTED tokens at 5s. Above that, the existing capacity adapter must fairly stretch protected cadence and report CRITICAL. The exact threshold varies with current arrival, WATCH, FADING-tail, batch occupancy and retry load and must be computed live.

## Burst and adversarial cases

No policy can give one million simultaneous admissions 15-second coverage inside 5,760 observations/min. V2 does not conceal this. It:

- enters DEGRADED/CRITICAL from projected class demand;
- deterministically stretches effective early-life cadence before creating due work;
- preserves at least one admission fact and a fair early observation opportunity;
- protects active/candidate allocations as far as possible;
- reports unserved target demand and realized coverage by admission cohort;
- never accepts infinite queue growth as a solution.

The near-bounded claim assumes a bounded recent arrival rate and bounded protected/candidate population. Status must show both assumptions. If arrivals grow without bound, finite per-token coverage still grows linearly with arrival rate; capacity adaptation remains necessary.

## Persisted policy/decision contract

Extend immutable scheduler policy/decisions rather than duplicating JSON on every row:

- coverage policy digest and immutable document;
- age/inactivity bands and direct-poll horizon;
- class priorities, reservations, scan caps and selection hash version;
- current counts by lifecycle, coverage class and age band;
- requested/effective observations and requests/min by class;
- target/effective interval by class and lifecycle;
- service reserve, safe budget, observed occupancy and occupancy threshold;
- capacity/coverage mode and reason;
- rebasing decisions for inherited schedules at epoch start;
- realized interval/lateness/starvation metrics by class and admission cohort.

Poll members reference the capacity and coverage decisions. A fixed policy hash plus immutable population decision must reconstruct why token X was or was not eligible at time T.

## Epoch initialization

Epochs do not own token identity, but timing metrics must not inherit an earlier epoch's overdue queue. A V2 epoch start should:

1. leave all historical facts and mutable cross-epoch token/pair metadata intact;
2. take an immutable start snapshot of eligible identities/lifecycle and policy;
3. cancel no historical obligations and rewrite no old facts;
4. create an epoch-scoped initialization decision that rebuilds current schedules from the V2 coverage policy and start time;
5. classify tokens beyond the direct horizon as retired scan candidates instead of immediately due;
6. stagger deterministic first due times across each interval/budget bucket;
7. ensure every new observation/request links to the new collector run/epoch.

## Validation tests and gates

- Fake-clock tests prove exactly 65 routine observations for an always-uninteresting token over seven days, plus only fixed-budget scans thereafter.
- 10k/50k/100k/500k/1m simulations reproduce or conservatively bound the table above.
- A 30-day accelerated arrival simulation shows eligible population and overdue age plateau.
- Four-worker tests prove deterministic fairness and no duplicate claims.
- Batch occupancy remains at most 30 and alerts below the required occupancy.
- ACTIVE overload becomes CRITICAL and degrades fairly.
- Restart/epoch initialization does not mass-reset tokens to NEW or import old lateness.
- Every class gets its promised allocation; starvation is evaluated by maximum rotations missed, not only average lateness.
- Realized requests, including availability and retries, remain below the configured safe budget.

## Unresolved tuning questions

- Revalidate the measured 23.9/min Epoch 2 admission rate and the 28/min stress assumption after a V2 dry simulation.
- Validate whether 5m after minute 10 loses meaningful pre-ACTIVE trajectories.
- Determine whether the FADING tail needs 30 or 60 minutes from Qenis-like collapses.
- Quantify event-feed resurrection recall against the fixed rotating control sample.
- Establish separate candidate-enrichment budgets before Tier 2/3 is activated.

These are validation parameters, not reasons to preserve the unbounded V1 long tail.
