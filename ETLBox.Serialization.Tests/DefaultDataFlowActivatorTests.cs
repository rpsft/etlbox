using System.Dynamic;
using System.Globalization;
using ALE.ETLBox.Serialization.DataFlow;
using CsvHelper.Configuration;
using FluentAssertions;

namespace ETLBox.Serialization.Tests;

public class DefaultDataFlowActivatorTests
{
    private readonly DefaultDataFlowActivator _activator = new();

    [Fact]
    public void CreateInstance_StandardType_ShouldCreateInstance()
    {
        var result = _activator.CreateInstance(typeof(List<string>));

        result.Should().NotBeNull();
        result.Should().BeOfType<List<string>>();
    }

    [Fact]
    public void CreateInstance_CsvConfiguration_ShouldCreateWithInvariantCulture()
    {
        var result = _activator.CreateInstance(typeof(CsvConfiguration));

        result.Should().NotBeNull();
        result.Should().BeOfType<CsvConfiguration>();
        var config = (CsvConfiguration)result!;
        config.CultureInfo.Should().Be(CultureInfo.InvariantCulture);
    }

    [Fact]
    public void CreateInstance_GenericTypeDefinition_ShouldConstructWithExpandoObject()
    {
        var result = _activator.CreateInstance(typeof(List<>));

        result.Should().NotBeNull();
        result.Should().BeOfType<List<ExpandoObject>>();
    }

    [Fact]
    public void CreateInstance_ConcreteGenericType_ShouldCreateDirectly()
    {
        var result = _activator.CreateInstance(typeof(List<int>));

        result.Should().NotBeNull();
        result.Should().BeOfType<List<int>>();
    }

    [Fact]
    public void CreateInstance_ExpandoObject_ShouldCreateInstance()
    {
        var result = _activator.CreateInstance(typeof(ExpandoObject));

        result.Should().NotBeNull();
        result.Should().BeOfType<ExpandoObject>();
    }
}
