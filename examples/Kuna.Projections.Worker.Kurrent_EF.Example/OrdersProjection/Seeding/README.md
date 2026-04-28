# OrdersProjection Seeding

This directory contains seeding code for `Kuna.Projections.Worker.Kurrent_EF.Example`.

Its purpose is narrow:

- generate example order event streams
- write those generated events to KurrentDB for the example worker
- optionally create example-only Kurrent snapshots used by the example workflow

This code is not part of the reusable projection pipeline.
It exists only to support the example project and its local scripts such as `scripts/seed-kurrent-live.sh`.
