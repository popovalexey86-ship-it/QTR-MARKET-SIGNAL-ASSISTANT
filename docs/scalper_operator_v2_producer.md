# Operator V2 producer

Base: 3358fa0420fce2e34007f1ef29c53e6f520ca37b.

Schema 2 adds recent_events (oldest first, maximum 20) and the independent
recent_event_failures counter. New readers accept exact V1 snapshots with an
empty feed. Existing V1 readers reject schema 2 explicitly; deploy a compatible
reader before switching a publisher. Total snapshot limit stays 128 KiB.

Capture observes existing ENTRY_CREATED, OPEN, terminal CLOSED and EXPIRED
runtime events. ENTRY_CREATED becomes ENTRY_CANDIDATE; it is not an executed
entry. EXPIRED has existing NOT_TRIGGERED status, without a fabricated price,
exit or R. CLOSED uses existing cumulative realized R and shadow_outcome;
BREAKEVEN is displayed as FLAT. Existing runtime reason codes are preserved.

Event IDs hash existing immutable runtime event identity and operator type.
The fixed deque retains the last 20 captured transitions, sorted by UTC time,
transition priority and ID at publication. Capture failures increment a separate
telemetry counter and cannot alter trading state, journals or returned results.
No startup reconstruction or JSONL reads are introduced; restart may lose the
in-memory feed. Repeated control refreshes only read the ring, never recapture.

Focused producer plus V1 control regression: 56 passed. No deployment/restart.

Unexpected operator projection/publication Exceptions are isolated at the CLI
operator boundary as well; they cannot stop the existing runtime.
