# Kuna.Projections.Worker.Kurrent_EF.Example

This example runs independently with:

- KurrentDB, seeded live with generated events
- PostgreSQL, used as the projection store

## Start Infrastructure

```bash
docker compose up -d
```

## Seed KurrentDB With 10,000 Generated Events

```bash
./scripts/seed-kurrent-live.sh
```

## Run The Worker

```bash
dotnet run -c Release --project ./Kuna.Projections.Worker.Kurrent_EF.Example.csproj
```

## Run The On-Demand Replay Consistency Check

See [CONSISTENCY_CHECK.md](./CONSISTENCY_CHECK.md).

## Restart Projection State Without Reseeding Events

```bash
./scripts/reset-projection-state.sh
```

## Reseed From Scratch

```bash
docker compose down -v
docker compose up -d
./scripts/seed-kurrent-live.sh
```

## Script Overrides

- `seed-kurrent-live.sh`: `TARGET_EVENTS`, `MIN_COMPLETE_ORDERS`, `STREAM_PREFIX`, `KURRENT_CONNECTION_STRING`, `REPORT_PATH`
- `reset-projection-state.sh`: `POSTGRES_CONTAINER_NAME`, `POSTGRES_USER`, `POSTGRES_DB`, `POSTGRES_SCHEMA`

## Configuration

- The worker uses `ConnectionStrings:PostgreSql` and `ConnectionStrings:KurrentDB` from `appsettings.json`.
