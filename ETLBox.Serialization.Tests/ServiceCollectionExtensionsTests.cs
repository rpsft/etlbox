using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Extensions;
using ALE.ETLBox.Serialization.DataFlow;
using ALE.ETLBox.Serialization.Extensions;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace ETLBox.Serialization.Tests;

/// <summary>
/// Tests for IServiceCollection extension methods that register ETLBox components.
/// </summary>
public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddEtlBoxCore_ShouldRegisterOpenGenericSources()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEtlBoxCore();

        AssertOpenGenericRegistered(services, typeof(DbSource<>));
        AssertOpenGenericRegistered(services, typeof(CsvSource<>));
        AssertOpenGenericRegistered(services, typeof(JsonSource<>));
        AssertOpenGenericRegistered(services, typeof(XmlSource<>));
        AssertOpenGenericRegistered(services, typeof(ExcelSource<>));
        AssertOpenGenericRegistered(services, typeof(MemorySource<>));
        AssertOpenGenericRegistered(services, typeof(CustomSource<>));
        AssertOpenGenericRegistered(services, typeof(CrossJoin<,,>));
    }

    [Fact]
    public void AddEtlBoxCore_ShouldRegisterOpenGenericTransformations()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEtlBoxCore();

        AssertOpenGenericRegistered(services, typeof(RowTransformation<,>));
        AssertOpenGenericRegistered(services, typeof(RowTransformation<>));
        AssertOpenGenericRegistered(services, typeof(BlockTransformation<,>));
        AssertOpenGenericRegistered(services, typeof(Multicast<>));
        AssertOpenGenericRegistered(services, typeof(Sort<>));
        AssertOpenGenericRegistered(services, typeof(RowDuplication<>));
        AssertOpenGenericRegistered(services, typeof(RowMultiplication<,>));
        AssertOpenGenericRegistered(services, typeof(Aggregation<,>));
        AssertOpenGenericRegistered(services, typeof(LookupTransformation<,>));
        AssertOpenGenericRegistered(services, typeof(MergeJoin<,,>));
        AssertOpenGenericRegistered(services, typeof(DbMerge<>));
    }

    [Fact]
    public void AddEtlBoxCore_ShouldRegisterOpenGenericDestinations()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEtlBoxCore();

        AssertOpenGenericRegistered(services, typeof(DbDestination<>));
        AssertOpenGenericRegistered(services, typeof(CsvDestination<>));
        AssertOpenGenericRegistered(services, typeof(JsonDestination<>));
        AssertOpenGenericRegistered(services, typeof(XmlDestination<>));
        AssertOpenGenericRegistered(services, typeof(MemoryDestination<>));
        AssertOpenGenericRegistered(services, typeof(CustomDestination<>));
        AssertOpenGenericRegistered(services, typeof(VoidDestination<>));
    }

    [Fact]
    public void AddEtlBoxCore_ShouldRegisterNonGenericShorthands()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEtlBoxCore();

        AssertRegistered<DbSource>(services);
        AssertRegistered<CsvSource>(services);
        AssertRegistered<JsonSource>(services);
        AssertRegistered<MemorySource>(services);
        AssertRegistered<CustomSource>(services);
        AssertRegistered<CrossJoin>(services);
        AssertRegistered<RowTransformation>(services);
        AssertRegistered<BlockTransformation>(services);
        AssertRegistered<Multicast>(services);
        AssertRegistered<Sort>(services);
        AssertRegistered<DbDestination>(services);
        AssertRegistered<MemoryDestination>(services);
        AssertRegistered<ErrorLogDestination>(services);
    }

    [Fact]
    public void AddEtlBoxCore_ShouldResolveCustomDtoType_WithoutExplicitRegistration()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEtlBoxCore();
        var provider = services.BuildServiceProvider();

        // These types were never explicitly registered — open generics resolve them
        var source = provider.GetRequiredService<DbSource<MyCustomRow>>();
        source.Should().NotBeNull();

        var dest = provider.GetRequiredService<DbDestination<MyCustomRow>>();
        dest.Should().NotBeNull();

        var sort = provider.GetRequiredService<Sort<MyCustomRow>>();
        sort.Should().NotBeNull();
    }

    [Fact]
    public void AddEtlBoxCore_ShouldResolveMultiTypeParamTransformation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEtlBoxCore();
        var provider = services.BuildServiceProvider();

        // RowTransformation<TInput, TOutput> with different input/output types
        var transform = provider.GetRequiredService<RowTransformation<MyCustomRow, AnotherRow>>();
        transform.Should().NotBeNull();
    }

    [Fact]
    public void AddEtlBoxCore_ShouldInjectLoggerThroughOpenGenerics()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEtlBoxCore();
        var provider = services.BuildServiceProvider();

        var source = provider.GetRequiredService<DbSource<MyCustomRow>>();
        source.Logger.Should().NotBeNull();
    }

    [Fact]
    public void AddEtlBoxCore_ShouldRegisterAsTransient()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEtlBoxCore();

        var descriptor = services.First(d => d.ServiceType == typeof(DbSource<>));
        descriptor.Lifetime.Should().Be(ServiceLifetime.Transient);
    }

    [Fact]
    public void AddEtlBoxSerialization_ShouldRegisterDataFlowXmlReader()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEtlBoxSerialization();

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(DataFlowXmlReader));
        descriptor.Should().NotBeNull();
        descriptor!.Lifetime.Should().Be(ServiceLifetime.Transient);
    }

    [Fact]
    public void AddEtlBoxCore_ShouldReturnSameServiceCollection()
    {
        var services = new ServiceCollection();

        var result = services.AddEtlBoxCore();

        result.Should().BeSameAs(services);
    }

    [Fact]
    public void AddEtlBoxSerialization_ShouldReturnSameServiceCollection()
    {
        var services = new ServiceCollection();

        var result = services.AddEtlBoxSerialization();

        result.Should().BeSameAs(services);
    }

    public class MyCustomRow
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    public class AnotherRow
    {
        public string Output { get; set; } = "";
    }

    private static void AssertRegistered<T>(IServiceCollection services)
    {
        services
            .Any(d => d.ServiceType == typeof(T) && d.Lifetime == ServiceLifetime.Transient)
            .Should()
            .BeTrue($"{typeof(T).Name} should be registered as transient");
    }

    private static void AssertOpenGenericRegistered(
        IServiceCollection services,
        Type openGenericType
    )
    {
        services
            .Any(d => d.ServiceType == openGenericType && d.Lifetime == ServiceLifetime.Transient)
            .Should()
            .BeTrue($"{openGenericType.Name} should be registered as open generic transient");
    }
}
