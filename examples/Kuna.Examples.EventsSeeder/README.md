# Kuna.Examples.EventsSeeder

This project owns the shared example event seeding workflow used by the runnable workers.

Its scope is narrow:

- generate example order event streams
- write those events to KurrentDB
- optionally create example Kurrent snapshots for local workflows

It is intentionally separate from the projection workers so EF and Mongo examples can reuse one seeding path.
