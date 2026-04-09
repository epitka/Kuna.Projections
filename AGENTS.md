# Repository Guidelines

## Project Structure & Module Organization
This repository contains a .NET 10 solution for event-sourcing projections.

- Core pipeline libraries: `Kuna.Projections.Abstractions`, `Kuna.Projections.Core`, `Kuna.Projections.Sink.EF`, `Kuna.Projections.Source.Kurrent`
- Example runnable worker: `examples/Kuna.Projections.Worker.Kurrent_EF.Example`
- Tests: `test/` (new pipeline)
- Benchmarks: `benchmarks/Kuna.Projections.Benchmarks`
- Utilities and scripts: `tools/`, `scripts/`

## Build, Test, and Development Commands
When Codex runs `dotnet` commands locally in this repository, include `-p:UseSharedCompilation=false`.
Do not add or propagate `-p:UseSharedCompilation=false` into repository files such as CI workflows, scripts, Makefiles, or documentation unless the user explicitly asks for that repository behavior change.
Run benchmark tests in `Release` mode only (never `Debug`).

- `dotnet restore -p:UseSharedCompilation=false Kuna.Projections.sln` - restore packages
- `dotnet build -c Release -p:UseSharedCompilation=false Kuna.Projections.sln` - build all projects
- `dotnet test -c Release --no-restore -p:UseSharedCompilation=false` - run all tests
- `dotnet test -c Release --no-restore -p:UseSharedCompilation=false test/Kuna.Projections.Core.Test/Kuna.Projections.Core.Test.csproj` - run core pipeline tests only
- `dotnet run -p:UseSharedCompilation=false --project examples/Kuna.Projections.Worker.Kurrent_EF.Example` - run showcase worker
- `dotnet run -c Release -p:UseSharedCompilation=false --project benchmarks/Kuna.Projections.Benchmarks` - run benchmarks

## Coding Style & Naming Conventions
- C# with 4-space indentation and UTF-8.
- Default to one type per file.
- Exception: when an interface and its default implementation live in the same project, keep them in the same file unless there is a strong reason to split them.
- Follow `stylecop.json` and `stylecop.ruleset`.
- Naming: `PascalCase` for types/methods/properties, `camelCase` for locals/parameters, interfaces prefixed with `I`.
- For `Guid` values, prefer `Guid.Empty` over `default`.
- Keep public APIs explicit and favor small, composable services.

## Testing Guidelines
- Test stack: xUnit v3, Shouldly, FakeItEasy; Testcontainers for integration tests.
- Test names follow `State_ExpectedBehavior` style only when there is only one method on the type, otherwise create a folder named by the type being tested suffixed with "Tests". For each method on type being tested create separate file named by the method under test. If possible, follow Given-When_Then/Should style for tests.
- Put tests in matching project folders under `test/`.
- For container/integration scenarios, use project-specific env flags when required (for example Kurrent/EF container tests).

## Commit & Pull Request Guidelines
- Use short, imperative commit subjects (seen in history: `benchmarks`, `integration tests passing`, `version check strategy`).
- Keep commits focused by concern (pipeline logic, tests, benchmarks, docs).
- PRs should include:
  - What changed and why
  - Affected projects/paths
  - Test evidence (`dotnet test` commands and results)
  - Config or migration notes for runtime behavior changes

## Planning
Store plans in /plans folder

## Slash Conventions
- `/todo`: Find TODO comments in the repository, present them one at a time in file order, and ask whether to implement each item before making any changes. Ignore backlog-style docs such as `plans/TODO.md` unless explicitly asked to include them.
