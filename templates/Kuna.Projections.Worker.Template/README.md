# Projection Worker Template

This template is a minimal starting point for a new Kurrent + EF projection worker.

It is intentionally derived from the orders example, but reduced to the smallest shape that still shows the full projection seam:

- host
- projection registration
- read model
- events
- projection
- EF projection store
- startup task
- configuration

## How to use it

1. Copy this folder to a new location.
2. Rename the project, namespace, and files to match your domain.
3. Replace the sample `Account` model and events with your own types.
4. Update `appsettings.json` connection strings and projection settings.
5. Add the new project to your solution.

## What to rename first

- project name: `Kuna.Projections.Worker.Template`
- namespace root: `Kuna.Projections.Worker.Template`
- projection section name: `AccountProjection`
- stream prefix: `account-`
- schema name: `account_projection`

## Why this template exists

The orders example is useful, but it also contains domain detail, diagnostics, and operational extras. This template keeps the same wiring pattern while making the projection seams easier to copy and adapt.
