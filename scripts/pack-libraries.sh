#!/usr/bin/env bash

set -euo pipefail

solution="Kuna.Projections.sln"
package_dir="artifacts/packages"

dotnet restore "$solution"
dotnet build -c Release --no-restore "$solution"

mkdir -p "$package_dir"

dotnet pack -c Release --no-build src/Kuna.Projections.Abstractions/Kuna.Projections.Abstractions.csproj -o "$package_dir"
dotnet pack -c Release --no-build src/Kuna.Projections.Core/Kuna.Projections.Core.csproj -o "$package_dir"
dotnet pack -c Release --no-build src/Kuna.Projections.Sink.EF/Kuna.Projections.Sink.EF.csproj -o "$package_dir"
dotnet pack -c Release --no-build src/Kuna.Projections.Sink.EF.Npgsql/Kuna.Projections.Sink.EF.Npgsql.csproj -o "$package_dir"
dotnet pack -c Release --no-build src/Kuna.Projections.Sink.EF.SqlServer/Kuna.Projections.Sink.EF.SqlServer.csproj -o "$package_dir"
dotnet pack -c Release --no-build src/Kuna.Projections.Sink.EF.MySql/Kuna.Projections.Sink.EF.MySql.csproj -o "$package_dir"
dotnet pack -c Release --no-build src/Kuna.Projections.Source.KurrentDB/Kuna.Projections.Source.KurrentDB.csproj -o "$package_dir"
