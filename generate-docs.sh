#!/usr/bin/env bash
set -euo pipefail
dotnet tool restore
dotnet build ETLBox.sln -c Release --no-incremental
cd docfx
dotnet docfx docfx.json
