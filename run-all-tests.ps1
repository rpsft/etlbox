#!/usr/bin/env pwsh

dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestConnectionManager
dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestControlFlowTasks
dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestDatabaseConnectors
dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestFlatFileConnectors
dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestHelper
dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestNonParallel
dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestOtherConnectors
dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestPerformance
dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestShared
dotnet test -c Release /p:CollectCoverage=true /p:CoverletOutputFormat=opencover --filter "Category!=Performance" ./TestTransformations