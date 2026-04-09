Kuna.Projections.Worker.ES_EF.Example showcase

This example runs independently with:
- KurrentDB (seeded live with generated events)
- PostgreSQL (projection store)

1) Start infrastructure
   docker compose up -d

2) Seed KurrentDB with 10,000 generated events
   ./scripts/seed-kurrent-live.sh

3) Run the worker
  dotnet run -c Release --project ./Kuna.Projections.Worker.Kurrent_EF.Example.csproj

4) Run the on-demand replay consistency check
   See `./CONSISTENCY_CHECK.md`

Notes:
- To restart the projection from the existing Kurrent event stream without reseeding,
  clear the projection tables:
  `./scripts/reset-projection-state.sh`
- To reseed from scratch, run `docker compose down -v && docker compose up -d` and seed again.
- `seed-kurrent-live.sh` supports overrides:
  `TARGET_EVENTS`, `MIN_COMPLETE_ORDERS`, `STREAM_PREFIX`, `KURRENT_CONNECTION_STRING`, `REPORT_PATH`
- `reset-projection-state.sh` supports overrides:
  `POSTGRES_CONTAINER_NAME`, `POSTGRES_USER`, `POSTGRES_DB`, `POSTGRES_SCHEMA`
- The worker uses ConnectionStrings:PostgreSql and ConnectionStrings:EventStore from appsettings.json.
