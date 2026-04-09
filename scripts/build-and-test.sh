#!/usr/bin/env bash

set -euo pipefail

solution="Kuna.Projections.sln"
results_dir="artifacts/test-results"

dotnet restore "$solution"
dotnet build -c Release --no-restore "$solution"

mkdir -p "$results_dir"

dotnet test -c Release --no-restore test/Kuna.Projections.Abstractions.Test/Kuna.Projections.Abstractions.Test.csproj --logger "trx;LogFileName=Kuna.Projections.Abstractions.Test.trx" --results-directory "$results_dir"
dotnet test -c Release --no-restore test/Kuna.Projections.Core.Test/Kuna.Projections.Core.Test.csproj --logger "trx;LogFileName=Kuna.Projections.Core.Test.trx" --results-directory "$results_dir"
