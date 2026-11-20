# Watchdog Telegram Alert Policy V1

This is an optional notification projector policy. It does not alter Watchdog
events, detectors, scores, market polling, baseline or outcome behavior. Enable
it only with `--alert-policy-v1` after a separate deployment review.

## Decision contract

- A journal event is always consumed and its ID/hash recorded in SQLite, even
  when its notification is suppressed. The original event journal remains the
  research record.
- An event older than 10 minutes, or more than 60 seconds in the future, is
  suppressed. This prevents a paused adapter from replaying old alerts.
- An immediate alert requires score >=55, or a transition into WATCH with at
  least two anomaly types and score >=50.
- Each symbol has a 12-hour cooldown. A score >=60 may break it only after 30
  minutes and a score increase of at least 15 points. A confirmed NORMAL→WATCH
  transition with at least two anomaly types and score >=50 can also break the
  cooldown after 30 minutes; this preserves distinct observed episodes.
- At most 36 immediate alerts are selected in a rolling 24-hour window.
- Suppressed current events contribute to a six-hour digest. With the Control
  Panel enabled, it reports the total number of events, three common detector
  types, up to five current instruments from a fresh snapshot and separate
  historical leaders. Without a fresh snapshot it says so explicitly.
  Digests that are more than one hour late expire without delivery. This means
  at most four digest messages in 24 hours, or 40 total selected messages when
  the immediate cap is reached.
- Selected alerts also expire 10 minutes after detection if Telegram delivery
  is delayed; the immutable event and its outbox evidence remain.
- Within the pending outbox, score >=60 alerts have priority over other
  immediate alerts; digests have the lowest priority. The existing at-least-once
  Telegram delivery contract and its documented ambiguous-acknowledgment
  duplicate risk remain unchanged.

The thresholds apply only to notification selection. A later read-only replay
of the 2026-11-19/20 live event journal through 08:31 UTC covered 6,198 unique
events: the original policy selected 25 immediate alerts; the unified policy
selected 29, including four new NORMAL→WATCH episodes. This is an observed
replay result, not a forecast or live delivery validation. No event was removed
from the journal. A separate review is required before deployment.

## Paused live adapter

The existing Telegram adapter was suspended without advancing its SQLite
cursor. Do not simply resume it: its old code would send the accumulated
backlog. Preserve that SQLite database and event journal. The post-soak switch
must use a single consumer, a separately reviewed policy-enabled unit, and a
new SQLite state initialized at the current journal EOF after a recorded
cutover boundary. The old SQLite remains archived. Start and verify the new
adapter before any Observer that requires its health. Do not copy baselines or
checkpoints between incompatible data roots.

## Review gates

Before any controlled deployment, confirm observer/adapter units, journal
device/inode/size, old cursor/outbox, new empty state, Telegram authorization,
MemoryAvailable, disk reserve, and no second consumer. Verify that suppressed
rows do not enter the delivery query, that the digest cannot replay old windows,
and that the Control Panel still reads all events via `panel_events` when
enabled. Observe outbox age/count and delivery errors after cutover. Roll back
by stopping only the new adapter and preserving both SQLite files; never resume
the old adapter until its backlog is explicitly reconciled.
