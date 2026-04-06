using System.Dynamic;
using ALE.ETLBox.Serialization.DataFlow;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace ETLBox.Serialization.Tests;

public class ServiceProviderActivatorTests
{
    [Fact]
    public void CreateInstance_RegisteredService_ShouldResolveFromContainer()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new TestService("injected"));
        var provider = services.BuildServiceProvider();
        var activator = new ServiceProviderActivator(provider);

        var result = activator.CreateInstance(typeof(TestService));

        result.Should().NotBeNull();
        result.Should().BeOfType<TestService>();
        ((TestService)result!).Name.Should().Be("injected");
    }

    [Fact]
    public void CreateInstance_UnregisteredTypeWithDefaultCtor_ShouldFallbackToActivatorUtilities()
    {
        var services = new ServiceCollection();
        var provider = services.BuildServiceProvider();
        var activator = new ServiceProviderActivator(provider);

        var result = activator.CreateInstance(typeof(SimpleClass));

        result.Should().NotBeNull();
        result.Should().BeOfType<SimpleClass>();
    }

    [Fact]
    public void CreateInstance_UnregisteredTypeWithDependency_ShouldInjectRegisteredDependencies()
    {
        var services = new ServiceCollection();
        services.AddSingleton(new TestService("fromDI"));
        var provider = services.BuildServiceProvider();
        var activator = new ServiceProviderActivator(provider);

        var result = activator.CreateInstance(typeof(ClassWithDependency));

        result.Should().NotBeNull();
        result.Should().BeOfType<ClassWithDependency>();
        ((ClassWithDependency)result!).Service.Name.Should().Be("fromDI");
    }

    [Fact]
    public void CreateInstance_GenericTypeDefinition_ShouldConstructWithExpandoObject()
    {
        var services = new ServiceCollection();
        var provider = services.BuildServiceProvider();
        var activator = new ServiceProviderActivator(provider);

        var result = activator.CreateInstance(typeof(List<>));

        result.Should().NotBeNull();
        result.Should().BeOfType<List<ExpandoObject>>();
    }

    [Fact]
    public void CreateInstance_ConcreteGenericType_ShouldCreateDirectly()
    {
        var services = new ServiceCollection();
        var provider = services.BuildServiceProvider();
        var activator = new ServiceProviderActivator(provider);

        var result = activator.CreateInstance(typeof(List<int>));

        result.Should().NotBeNull();
        result.Should().BeOfType<List<int>>();
    }

    [Fact]
    public void Constructor_NullServiceProvider_ShouldThrowArgumentNullException()
    {
        var act = () => new ServiceProviderActivator(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("serviceProvider");
    }

    public class TestService
    {
        public string Name { get; }

        public TestService(string name) => Name = name;
    }

    public class SimpleClass
    {
        public string Value { get; set; } = "default";
    }

    public class ClassWithDependency
    {
        public TestService Service { get; }

        public ClassWithDependency(TestService service) => Service = service;
    }
}
