using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Xml;
using ALE.ETLBox.Serialization;
using ALE.ETLBox.Serialization.DataFlow;

namespace ETLBox.Serialization.Tests;

/// <summary>
/// Tests for IDataFlow disposable resource ownership: GetOrAddResource and Dispose behavior,
/// plus DataFlowXmlReader integration that auto-registers IDisposable properties.
/// </summary>
public sealed class DataFlowDisposableResourceTests
{
    // ── Pure EtlDataFlowStep unit tests ─────────────────────────────────────

    [Fact]
    public void GetOrAddResource_SameKey_ReturnsSameInstance()
    {
        using var step = new EtlDataFlowStep();

        var first = step.GetOrAddResource("key1", () => new TrackableResource { Config = "a" });
        var second = step.GetOrAddResource("key1", () => new TrackableResource { Config = "b" });

        Assert.Same(first, second);
    }

    [Fact]
    public void GetOrAddResource_DifferentKeys_ReturnDifferentInstances()
    {
        using var step = new EtlDataFlowStep();

        var first = step.GetOrAddResource("key1", () => new TrackableResource { Config = "a" });
        var second = step.GetOrAddResource("key2", () => new TrackableResource { Config = "a" });

        Assert.NotSame(first, second);
    }

    [Fact]
    public void GetOrAddResource_Generic_ReturnTypedInstance()
    {
        using var step = new EtlDataFlowStep();

        var result = step.GetOrAddResource("key1", () => new TrackableResource { Config = "x" });

        Assert.IsType<TrackableResource>(result);
        Assert.Equal("x", ((TrackableResource)result).Config);
    }

    [Fact]
    public void GetOrAddResource_GenericExtension_ReturnTypedInstance()
    {
        using var step = new EtlDataFlowStep();

        var result = DataFlowExtensions.GetOrAddResource(
            step,
            "key1",
            () => new TrackableResource { Config = "y" }
        );

        Assert.IsType<TrackableResource>(result);
        Assert.Equal("y", result.Config);
    }

    [Fact]
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope")]
    public void Dispose_DisposesRegisteredResource()
    {
        var resource = new TrackableResource();
        var step = new EtlDataFlowStep();
        step.GetOrAddResource("r1", () => resource);

        step.Dispose();

        Assert.True(resource.IsDisposed);
    }

    [Fact]
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope")]
    public void Dispose_MultipleResources_AllDisposed()
    {
        var r1 = new TrackableResource();
        var r2 = new TrackableResource();
        var step = new EtlDataFlowStep();
        step.GetOrAddResource("r1", () => r1);
        step.GetOrAddResource("r2", () => r2);

        step.Dispose();

        Assert.True(r1.IsDisposed);
        Assert.True(r2.IsDisposed);
    }

    [Fact]
    public void ResourceCount_ReflectsAddedResources()
    {
        using var step = new EtlDataFlowStep();
        step.GetOrAddResource("r1", () => new TrackableResource());
        step.GetOrAddResource("r2", () => new TrackableResource());

        Assert.Equal(2, step.ResourceCount());
    }

    [Fact]
    public void ResourceCount_SameKeyNotDuplicated()
    {
        using var step = new EtlDataFlowStep();
        step.GetOrAddResource("r1", () => new TrackableResource());
        step.GetOrAddResource("r1", () => new TrackableResource());

        Assert.Equal(1, step.ResourceCount());
    }

    // ── XML integration: SetClassProperty path (concrete IDisposable) ────────

    [Fact]
    public void XmlReader_ConcreteDisposableStepProperty_RegisteredWithFlow()
    {
        var xml =
            @"<StepWithConcreteResource>
                <Resource>
                    <Config>hello</Config>
                </Resource>
            </StepWithConcreteResource>";

        var step = DeserializeStep<StepWithConcreteResource>(xml);

        Assert.Equal(1, step.ResourceCount());
        Assert.NotNull(step.Resource);
        Assert.Equal("hello", step.Resource!.Config);
    }

    [Fact]
    public void XmlReader_SameConcreteDisposableConfig_SharedInstance()
    {
        // Two elements with identical XML → same resource instance (dedup by content key)
        var xml =
            @"<StepWithTwoResources>
                <First>
                    <Config>shared</Config>
                </First>
                <Second>
                    <Config>shared</Config>
                </Second>
            </StepWithTwoResources>";

        var step = DeserializeStep<StepWithTwoResources>(xml);

        Assert.Equal(1, step.ResourceCount());
        Assert.Same(step.First, step.Second);
    }

    [Fact]
    public void XmlReader_DifferentConcreteDisposableConfig_SeparateInstances()
    {
        var xml =
            @"<StepWithTwoResources>
                <First>
                    <Config>config-a</Config>
                </First>
                <Second>
                    <Config>config-b</Config>
                </Second>
            </StepWithTwoResources>";

        var step = DeserializeStep<StepWithTwoResources>(xml);

        Assert.Equal(2, step.ResourceCount());
        Assert.NotSame(step.First, step.Second);
    }

    [Fact]
    public void XmlReader_ConcreteDisposableStepProperty_DisposedWithFlow()
    {
        var xml =
            @"<StepWithConcreteResource>
                <Resource>
                    <Config>test</Config>
                </Resource>
            </StepWithConcreteResource>";

        var step = DeserializeStep<StepWithConcreteResource>(xml);
        var resource = step.Resource!;

        step.Dispose();

        Assert.True(resource.IsDisposed);
    }

    // ── XML integration: SetInterfaceProperty path (abstract IDisposable) ────

    [Fact]
    public void XmlReader_AbstractDisposableNestedProperty_RegisteredWithFlow()
    {
        var xml =
            @"<StepWithHostComponent>
                <Component>
                    <Resource type=""ConcreteResource"">
                        <Config>hello</Config>
                    </Resource>
                </Component>
            </StepWithHostComponent>";

        var step = DeserializeStep<StepWithHostComponent>(xml);

        Assert.Equal(1, step.ResourceCount());
        Assert.NotNull(step.Component?.Resource);
        Assert.Equal("hello", step.Component!.Resource!.Config);
    }

    [Fact]
    public void XmlReader_SameAbstractDisposableConfig_SharedInstance()
    {
        var xml =
            @"<StepWithTwoHostComponents>
                <First>
                    <Resource type=""ConcreteResource"">
                        <Config>shared</Config>
                    </Resource>
                </First>
                <Second>
                    <Resource type=""ConcreteResource"">
                        <Config>shared</Config>
                    </Resource>
                </Second>
            </StepWithTwoHostComponents>";

        var step = DeserializeStep<StepWithTwoHostComponents>(xml);

        Assert.Equal(1, step.ResourceCount());
        Assert.Same(step.First!.Resource, step.Second!.Resource);
    }

    [Fact]
    public void XmlReader_AbstractDisposableNestedProperty_DisposedWithFlow()
    {
        var xml =
            @"<StepWithHostComponent>
                <Component>
                    <Resource type=""ConcreteResource"">
                        <Config>test</Config>
                    </Resource>
                </Component>
            </StepWithHostComponent>";

        var step = DeserializeStep<StepWithHostComponent>(xml);
        var resource = (ConcreteResource)step.Component!.Resource!;

        step.Dispose();

        Assert.True(resource.IsDisposed);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static T DeserializeStep<T>(string xml)
        where T : EtlDataFlowStep, new()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(xml));
        using var xmlReader = XmlReader.Create(stream);
        var step = new T();
        var reader = new DataFlowXmlReader(step);
        reader.Read(xmlReader);
        return step;
    }

    // ── test fixtures ────────────────────────────────────────────────────────

    public sealed class TrackableResource : IDisposable
    {
        public bool IsDisposed { get; private set; }
        public string? Config { get; set; }

        public void Dispose() => IsDisposed = true;
    }

    public abstract class AbstractResource : IDisposable
    {
        public string? Config { get; set; }
        public bool IsDisposed { get; private set; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
                IsDisposed = true;
        }
    }

    public sealed class ConcreteResource : AbstractResource { }

    public sealed class ComponentWithAbstractResource
    {
        public AbstractResource? Resource { get; set; }
    }

    public sealed class StepWithConcreteResource : EtlDataFlowStep
    {
        public TrackableResource? Resource { get; set; }
    }

    public sealed class StepWithTwoResources : EtlDataFlowStep
    {
        public TrackableResource? First { get; set; }
        public TrackableResource? Second { get; set; }
    }

    public sealed class StepWithHostComponent : EtlDataFlowStep
    {
        public ComponentWithAbstractResource? Component { get; set; }
    }

    public sealed class StepWithTwoHostComponents : EtlDataFlowStep
    {
        public ComponentWithAbstractResource? First { get; set; }
        public ComponentWithAbstractResource? Second { get; set; }
    }
}
