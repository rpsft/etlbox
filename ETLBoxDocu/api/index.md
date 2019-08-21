# API Documentation

## Welcome 

Welcome to the API documentation of ETLBox. This will give you access to all class and interface definitions that come with ETLBox.
Check the [Github page](https://github.com/roadrunnerlenny/etlbox) to see the full source code.

## .NET Standard App

ETLBox is .NET Standard app (.NET Standard 2.0 or higher).

## Namespaces

ETLBox is divided in several namepsace. 

The important ones are the namespace for the Control Flow `ALE.ETLBox.ControlFlow` where the Control Flow Tasks reside. 
All Data Flow Tasks (sometimes referred as components) can be found in the namespace `ALE.ETLBox.DataFlow`.
You'll find tasks related to logging in the namespace `ALE.ETLBox.Logging`.
Classes with helpful (mostly static) methods are in the namespace `ALE.ETLBox.Helper`.
To establish connection to a database, file or Analysis Services you'll need a connection manager from the `ALE.ETLBox.ConnectionManager` namespace.
Everything else like generic classes or definitions are in the namespace `ALE.ETLBox`.

## Docfx

The API documentation was created with [DocFX](https://dotnet.github.io/docfx/).
