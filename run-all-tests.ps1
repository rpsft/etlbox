#!/usr/bin/env pwsh

$options = @("-c", "Release", "/p:CollectCoverage=true", "/p:CoverletOutputFormat=opencover", "--filter", "Category!=Performance", "--no-build", "--logger", "console;verbosity=detailed", '--collect:"XPlat Code Coverage"')

dotnet test ./TestConnectionManager $options
dotnet test ./TestControlFlowTasks $options
dotnet test ./TestDatabaseConnectors $options
dotnet test ./TestFlatFileConnectors $options
dotnet test ./TestHelper $options
dotnet test ./TestNonParallel $options
dotnet test ./TestOtherConnectors $options
dotnet test ./TestPerformance $options
dotnet test ./TestShared $options
dotnet test ./TestTransformations $options