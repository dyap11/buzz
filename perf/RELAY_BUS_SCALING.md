# Buzz relay bus scaling harness

This harness gives reproducible evidence for the rewrite's Redis fan-out scaling claim:

- **old/global bus:** every relay pod receives every community's event;
- **new/community-scoped bus:** each pod retains only the server-resolved community topics for which it has local subscribers (`buzz:{community_id}:global` or `buzz:{community_id}:channel:{channel_id}`).

The measured default uses Redis PUB/SUB directly with simulated relay pod subscribers. It intentionally isolates the bus boundary: no DB ingest, websocket framing, client rendering, or relay business logic is included.

## Run the measured Redis harness

Start Redis locally, or point at an existing instance with `REDIS_URL`:

```bash
REDIS_URL=redis://127.0.0.1:6379/0 ./perf/relay_bus_scaling.py --mode redis
```

`--mode redis` is the default. The script uses only Python stdlib and speaks RESP directly; no Python Redis dependency is required.

Credentials in `REDIS_URL` are percent-decoded before `AUTH` is sent, so a password containing `@`, `:` or `/` must be URL-encoded in the URL and arrives at Redis in its literal form:

```bash
# user "us@er", password "p@ss:w/rd", database 2
REDIS_URL='redis://us%40er:p%40ss%3Aw%2Frd@redis.internal:6380/2' ./perf/relay_bus_scaling.py
```

Only `redis://` is supported; `rediss://` (TLS) is rejected.

Baseline scenario used for the PR summary:

```text
64 communities × 100 events/s, one subscribed community, all pods interested in that community, pods = 1,2,4
```

Measured output shape:

| pods | old global cluster ingress/s | old avg pod ingress/s | new scoped cluster ingress/s | new avg pod ingress/s | reduction | old irrelevant/pod | new irrelevant/pod |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 6,400 | 6,400 | 100 | 100 | 64.0× | 98.44% | 0.00% |
| 2 | 12,800 | 6,400 | 200 | 100 | 64.0× | 98.44% | 0.00% |
| 4 | 25,600 | 6,400 | 400 | 100 | 64.0× | 98.44% | 0.00% |

The harness fails non-zero by default unless:

- every row actually carried traffic (published count and both old and new cluster delivery counts are positive, reduction is finite);
- observed reduction is at least 95% of the ideal `communities / subscribed_communities`; and
- scoped-mode irrelevant delivery is at most `--max-scoped-irrelevant-pct` (default `0.0`).

That makes the scaling claim load-bearing: if scoped subscribers are accidentally changed to receive the old global firehose, the assertion goes red. The zero-delivery guard closes the opposite hole: a scoped bus that delivers nothing at all would otherwise report an infinite reduction and 0% irrelevant traffic and pass.

## Input validation

Scenario arguments are checked before any Redis connection is opened. The script exits with status 2 and a usage error when:

- `--events-per-community-per-sec` is not a finite positive number (`nan`, `inf`, `0`, negatives), or is non-integer in `--mode redis`;
- `--interested-pods-per-subscribed-community` is negative (`0` still means "all pods");
- `--communities` or `--subscribed-communities` is not positive, or subscribed exceeds communities;
- `--pods` contains a non-positive count.

In redis mode the expected delivery total passed to the count wait must also be positive; a non-positive target raises instead of being trivially satisfied by zero deliveries.

## No-service model mode

For quick review without Redis:

```bash
./perf/relay_bus_scaling.py --mode model
```

Model mode prints the same contract using deterministic arithmetic. It is useful for docs and unit tests, but the PR evidence should cite `--mode redis` because that path measures actual Redis PUB/SUB delivery.

## Unit tests

```bash
python3 -m unittest discover -s perf -p 'test_*.py'
```

The tests are standard library only and never contact Redis. They cover:

- the default 1/2/4-pod 64× contract, a mutant row where scoped mode receives irrelevant global-firehose traffic, and mutant rows with zero deliveries or a non-finite reduction, all of which must fail the assertion;
- a table of event rates and interested-pod counts across both modes, plus redis-mode `run()` calls that must raise before `socket.create_connection` is reached;
- byte-exact RESP command encoding and reply parsing (simple string, error, integer, bulk, null bulk, nested array, malformed and truncated frames, EOF);
- `REDIS_URL` parsing with percent-encoded credentials and scheme rejection;
- `RedisClient` and `RedisSubscriber` driven over an in-memory fake transport, asserting the exact `AUTH`/`SELECT`/`SUBSCRIBE` frames written and the relevant/irrelevant message counts.

## Code provenance

The scoped Redis channel format corresponds to `buzz_pubsub::EventTopicKey::redis_channel()` in `crates/buzz-pubsub/src/topic.rs`:

- global: `buzz:{community_id}:global`
- channel: `buzz:{community_id}:channel:{channel_id}`

`retain_topic` / `release_topic` drive dynamic local Redis `SUBSCRIBE` interest. This harness measures that bus-bound property only. Live relay latency, DB capacity, and client rendering should be measured separately with a full stack because they include unrelated bottlenecks.
