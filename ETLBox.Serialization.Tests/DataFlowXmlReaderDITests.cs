using System.Dynamic;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Serialization.DataFlow;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;

namespace ETLBox.Serialization.Tests;

/// <summary>
/// Tests for DataFlowXmlReader DI support (IServiceProvider, IDataFlowActivator).
/// </summary>
public class DataFlowXmlReaderDITests
{
    [Fact]
    public void Constructor_WithServiceProvider_ShouldUseServiceProviderActivator()
    {
        var services = new ServiceCollection();
        var provider = services.BuildServiceProvider();
        using var dataFlow = new EtlDataFlowStep();

        var reader = new DataFlowXmlReader(dataFlow, serviceProvider: provider);

        reader.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithoutServiceProvider_ShouldUseDefaultActivator()
    {
        using var dataFlow = new EtlDataFlowStep();

        var reader = new DataFlowXmlReader(dataFlow);

        reader.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithCustomActivator_ShouldUseProvidedActivator()
    {
        using var dataFlow = new EtlDataFlowStep();
        var activator = new DefaultDataFlowActivator();

        var reader = new DataFlowXmlReader(dataFlow, activator);

        reader.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullActivator_ShouldThrowArgumentNullException()
    {
        using var dataFlow = new EtlDataFlowStep();

        var act = () => new DataFlowXmlReader(dataFlow, (IDataFlowActivator)null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("activator");
    }

    [Fact]
    public void Read_WithServiceProvider_ShouldDeserializeComponents()
    {
        // Arrange
        var xml =
            @"<EtlDataFlowStepWithDI>
                <MemorySource>
                    <LinkTo>
                        <MemoryDestination />
                    </LinkTo>
                </MemorySource>
            </EtlDataFlowStepWithDI>";

        using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
        var serializer = new XmlSerializer(typeof(EtlDataFlowStepWithDI));
        var step = (EtlDataFlowStepWithDI)serializer.Deserialize(stream)!;

        // Assert
        step.Should().NotBeNull();
        step.Source.Should().NotBeNull();
        step.Source.Should().BeAssignableTo<MemorySource<ExpandoObject>>();
        step.Destinations.Should().HaveCount(1);
    }

    [Fact]
    public void Read_WithCustomActivator_ShouldUseActivator()
    {
        // Arrange - use a tracking activator to verify it was called
        var trackingActivator = new TrackingActivator();

        var xml =
            @"<EtlDataFlowStep>
                <MemorySource>
                    <LinkTo>
                        <MemoryDestination />
                    </LinkTo>
                </MemorySource>
            </EtlDataFlowStep>";

        using var dataFlow = new EtlDataFlowStep();
        using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
        using var xmlReader = XmlReader.Create(stream);
        var reader = new DataFlowXmlReader(dataFlow, trackingActivator);
        reader.Read(xmlReader);

        // Assert
        trackingActivator.CreatedTypes.Should().NotBeEmpty();
    }

    [Fact]
    public void Deserialize_WithServiceProvider_ShouldWork()
    {
        // Arrange
        var services = new ServiceCollection();
        var provider = services.BuildServiceProvider();
        var errorDest = new ErrorLogDestination();

        var xml =
            @"<EtlDataFlowStep>
                <MemorySource>
                    <LinkTo>
                        <MemoryDestination />
                    </LinkTo>
                </MemorySource>
            </EtlDataFlowStep>";

        // Act
        var step = DataFlowXmlReader.Deserialize<EtlDataFlowStep>(xml, errorDest, provider);

        // Assert
        step.Should().NotBeNull();
        step.Source.Should().NotBeNull();
    }

    [Fact]
    public void Deserialize_WithNullServiceProvider_ShouldUseDefaultActivator()
    {
        // Arrange
        var errorDest = new ErrorLogDestination();

        var xml =
            @"<EtlDataFlowStep>
                <MemorySource>
                    <LinkTo>
                        <MemoryDestination />
                    </LinkTo>
                </MemorySource>
            </EtlDataFlowStep>";

        // Act
        var step = DataFlowXmlReader.Deserialize<EtlDataFlowStep>(xml, errorDest, null);

        // Assert
        step.Should().NotBeNull();
        step.Source.Should().NotBeNull();
    }

    /// <summary>
    /// Custom EtlDataFlowStep that uses IServiceProvider for DI.
    /// </summary>
    [Serializable]
    public class EtlDataFlowStepWithDI : EtlDataFlowStep
    {
        public override void ReadXml(XmlReader reader)
        {
            var services = new ServiceCollection();
            var provider = services.BuildServiceProvider();
            var xmlReader = new DataFlowXmlReader(this, serviceProvider: provider);
            xmlReader.Read(reader);
        }
    }

    /// <summary>
    /// Activator that tracks which types it creates, delegating to DefaultDataFlowActivator.
    /// </summary>
    private class TrackingActivator : IDataFlowActivator
    {
        private readonly DefaultDataFlowActivator _inner = new();
        public List<Type> CreatedTypes { get; } = [];

        public object? CreateInstance(Type type)
        {
            CreatedTypes.Add(type);
            return _inner.CreateInstance(type);
        }
    }
}
