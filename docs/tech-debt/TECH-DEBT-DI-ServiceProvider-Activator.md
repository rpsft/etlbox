# Tech Debt: DI-based Activator Mode for DataFlowXmlReader

## Summary

Implement an alternative activation mode for `DataFlowXmlReader` that uses Microsoft Dependency Injection (`IServiceProvider`) instead of the current `DataFlowActivator` approach.

## Current Implementation

- **File**: `ETLBox.Serialization/DataFlow/DataFlowXmlReader.cs`
- **Activator**: `ETLBox.Serialization/DataFlow/DataFlowActivator.cs`

Currently, `DataFlowXmlReader` uses `DataFlowActivator.CreateInstance(type)` to instantiate types during XML deserialization. The `DataFlowActivator` relies on `Activator.CreateInstance()` which only supports parameterless constructors (with special cases for known types like `CsvConfiguration`).

### Affected Code Locations

```csharp
// DataFlowXmlReader.cs:262
var list = DataFlowActivator.CreateInstance(type);

// DataFlowXmlReader.cs:280
var instance = DataFlowActivator.CreateInstance(type);
```

## Proposed Enhancement

Add an optional `IServiceProvider` parameter to `DataFlowXmlReader` that enables DI-based instance creation:

### Benefits

1. **Extensibility**: New data flow steps can have constructor dependencies injected
2. **Service Integration**: Steps can depend on services registered in the DI container (logging, configuration, caching, etc.)
3. **Testability**: Easier to mock dependencies when testing custom steps
4. **Consistency**: Aligns with modern .NET application patterns

### Suggested Implementation Approach

1. **Add optional `IServiceProvider` to `DataFlowXmlReader` constructor**:
   ```csharp
   public DataFlowXmlReader(
       IDataFlow dataFlow,
       CultureInfo? currentCulture = null,
       IDataFlowDestination<ETLBoxError>? linkAllErrorsTo = null,
       IServiceProvider? serviceProvider = null  // NEW
   )
   ```

2. **Create `IDataFlowActivator` interface**:
   ```csharp
   public interface IDataFlowActivator
   {
       object? CreateInstance(Type type);
   }
   ```

3. **Implement `ServiceProviderActivator`**:
   ```csharp
   public class ServiceProviderActivator : IDataFlowActivator
   {
       private readonly IServiceProvider _serviceProvider;

       public ServiceProviderActivator(IServiceProvider serviceProvider)
       {
           _serviceProvider = serviceProvider;
       }

       public object? CreateInstance(Type type)
       {
           // First try to resolve from the container to respect registered lifetimes
           // (Transient/Scoped/Singleton). Fall back to ActivatorUtilities.CreateInstance
           // for types not registered in the container.
           return _serviceProvider.GetService(type)
               ?? ActivatorUtilities.CreateInstance(_serviceProvider, type);
       }
   }
   ```

4. **Refactor existing `DataFlowActivator` to implement `IDataFlowActivator`**:
   ```csharp
   public class DefaultDataFlowActivator : IDataFlowActivator
   {
       public object? CreateInstance(Type type) { /* existing logic */ }
   }
   ```

5. **Update `DataFlowXmlReader` to use `IDataFlowActivator`**:
   ```csharp
   private readonly IDataFlowActivator _activator;

   public DataFlowXmlReader(...)
   {
       _activator = serviceProvider != null
           ? new ServiceProviderActivator(serviceProvider)
           : new DefaultDataFlowActivator();
   }
   ```

### Usage Example

```csharp
// Register custom step with dependencies
services.AddTransient<MyCustomSource>();
services.AddSingleton<IMyService, MyService>();

// Create reader with DI support
var serviceProvider = services.BuildServiceProvider();
var reader = new DataFlowXmlReader(dataFlow, serviceProvider: serviceProvider);
```

```csharp
// Custom step with constructor injection
public class MyCustomSource : DataFlowSource<ExpandoObject>
{
    private readonly IMyService _myService;

    public MyCustomSource(IMyService myService)
    {
        _myService = myService;
    }
}
```

## Part 2: Service Collection Registration Extensions

Each ETLBox library should provide extension methods for `IServiceCollection` to register all data flow steps.

### Implementation Plan

1. **Create extension class per library**:
   ```csharp
   // In ETLBox.Core or ETLBox
   public static class EtlBoxServiceCollectionExtensions
   {
       public static IServiceCollection AddEtlBoxCore(this IServiceCollection services)
       {
           // Register all core data flow components
           services.AddTransient<DbSource<ExpandoObject>>();
           services.AddTransient<DbDestination<ExpandoObject>>();
           services.AddTransient<RowTransformation<ExpandoObject>>();
           // ... all other core steps
           return services;
       }
   }
   ```

2. **Library-specific extensions**:
   ```csharp
   // ETLBox.Csv
   public static class EtlBoxCsvServiceCollectionExtensions
   {
       public static IServiceCollection AddEtlBoxCsv(this IServiceCollection services)
       {
           services.AddTransient<CsvSource<ExpandoObject>>();
           services.AddTransient<CsvDestination<ExpandoObject>>();
           return services;
       }
   }

   // ETLBox.Json
   public static class EtlBoxJsonServiceCollectionExtensions
   {
       public static IServiceCollection AddEtlBoxJson(this IServiceCollection services)
       {
           services.AddTransient<JsonSource<ExpandoObject>>();
           services.AddTransient<JsonDestination<ExpandoObject>>();
           return services;
       }
   }

   // ETLBox.Xml
   public static class EtlBoxXmlServiceCollectionExtensions
   {
       public static IServiceCollection AddEtlBoxXml(this IServiceCollection services)
       {
           services.AddTransient<XmlSource<ExpandoObject>>();
           services.AddTransient<XmlDestination<ExpandoObject>>();
           return services;
       }
   }

   // ETLBox.Serialization
   public static class EtlBoxSerializationServiceCollectionExtensions
   {
       public static IServiceCollection AddEtlBoxSerialization(this IServiceCollection services)
       {
           services.AddTransient<DataFlowXmlReader>();
           return services;
       }
   }
   ```

3. **Aggregate registration method** *(example for consuming applications, not part of ETLBox packages)*:

   > **Note:** This extension is shown as a reference for third-party consumers who want a single registration call across multiple ETLBox packages. It should NOT be shipped inside any individual ETLBox NuGet package, as it would introduce dependencies on all other ETLBox packages, defeating the purpose of the modular package structure.

   ```csharp
   // Example: consumers can create this in their own project
   public static class EtlBoxServiceCollectionExtensions
   {
       public static IServiceCollection AddEtlBox(this IServiceCollection services)
       {
           services.AddEtlBoxCore();
           services.AddEtlBoxCsv();
           services.AddEtlBoxJson();
           services.AddEtlBoxXml();
           services.AddEtlBoxSerialization();
           // ... other libraries as needed
           return services;
       }
   }
   ```

### Libraries Requiring Registration Extensions

- ETLBox (core)
- ETLBox.Csv
- ETLBox.Json
- ETLBox.Xml
- ETLBox.Excel
- ETLBox.Parquet
- ETLBox.Serialization
- ETLBox.Azure.* (if applicable)
- ETLBox.* (all provider-specific libraries)

## Part 3: ILogger Constructor Support for All Steps

Add an optional `ILogger` parameter to constructors of all data flow steps to enable structured logging.

### Implementation Pattern

1. **Add `ILogger` field to base classes or each step**:
   ```csharp
   public class DbSource<TOutput> : DataFlowSource<TOutput>
   {
       private readonly ILogger? _logger;

       // Existing parameterless constructor (preserved for backward compatibility)
       public DbSource() : this(null) { }

       // New constructor with ILogger
       public DbSource(ILogger<DbSource<TOutput>>? logger)
       {
           _logger = logger;
       }

       // Alternative: non-generic ILogger for easier DI registration
       public DbSource(ILogger? logger)
       {
           _logger = logger;
       }
   }
   ```

2. **Consider base class approach**:
   ```csharp
   public abstract class DataFlowComponent
   {
       protected ILogger? Logger { get; }

       protected DataFlowComponent(ILogger? logger = null)
       {
           Logger = logger;
       }
   }
   ```

3. **Usage in steps**:
   ```csharp
   public class RowTransformation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>
   {
       private readonly ILogger? _logger;

       public RowTransformation() : this(null) { }

       public RowTransformation(ILogger? logger)
       {
           _logger = logger;
       }

       protected override void ProcessRow(TInput row)
       {
           _logger?.LogDebug("Processing row: {RowType}", typeof(TInput).Name);
           // ... existing logic
       }
   }
   ```

### Affected Step Categories

All data flow components should receive this enhancement:

**Sources**:
- DbSource, CsvSource, JsonSource, XmlSource, ExcelSource, ParquetSource
- CustomSource, MemorySource, etc.

**Transformations**:
- RowTransformation, ColumnTransformation, Aggregation
- Lookup, MergeJoin, CrossJoin, Sort, Distinct
- BatchTransformation, BlockTransformation, etc.

**Destinations**:
- DbDestination, CsvDestination, JsonDestination, XmlDestination
- ExcelDestination, ParquetDestination, CustomDestination, MemoryDestination, etc.

### Logging Integration Points

Consider logging at these points within each step:
- Initialization/configuration
- Start of execution
- Progress (batches processed, rows processed)
- Errors and exceptions
- Completion statistics

## Priority

Medium - This is an enhancement that improves extensibility but doesn't block current functionality.

## Implementation Order

1. Create `IDataFlowActivator` interface and implementations
2. Update `DataFlowXmlReader` to use activator abstraction
3. Add `ILogger` constructors to all data flow steps (can be done incrementally)
4. Create `IServiceCollection` registration extensions per library
5. Create aggregate registration extension
6. Update documentation and examples

## Related Files

- `ETLBox.Serialization/DataFlow/DataFlowXmlReader.cs`
- `ETLBox.Serialization/DataFlow/DataFlowActivator.cs`
- All source, transformation, and destination classes across ETLBox libraries

## Dependencies

- `Microsoft.Extensions.DependencyInjection.Abstractions` (for `IServiceProvider`, `IServiceCollection`)
- `Microsoft.Extensions.DependencyInjection` (for `ActivatorUtilities`)
- `Microsoft.Extensions.Logging.Abstractions` (for `ILogger`, `ILogger<T>`)
