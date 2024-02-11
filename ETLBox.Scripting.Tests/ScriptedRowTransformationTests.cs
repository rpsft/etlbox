using System.Dynamic;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Scripting;
using ETLBox.Primitives;
using JetBrains.Annotations;

namespace ETLBox.Scripting.Tests;

public class ScriptedRowTransformationTests
{
    [Fact]
    public void ShouldTransformExpandoObject()
    {
        // Arrange
        var memorySource = new MemorySource();
        memorySource.DataAsList.Add(CreateTestDataItem(1, "Test"));
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>();
        script.Mappings.Add("NewId", "Id + 1");
        script.Mappings.Add("NewName", "$\"New{Name}\"");
        var memoryDestination = new MemoryDestination<ExpandoObject>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);
        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();
        // Assert
        Assert.Single(
            memoryDestination.Data,
            (dynamic row) => row.NewId == 2 && row.NewName == "NewTest"
        );
    }

    [Fact]
    public void ShouldTransformExpandoObjectWithMissingFieldOnSource()
    {
        // Arrange
        var memorySource = new MemorySource();
        memorySource.DataAsList.Add(CreateTestDataItem(1, "Test"));
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>
        {
            FailOnMissingField = false
        };
        script.Mappings.Add("NewId", "Id + 1");
        script.Mappings.Add("NewName", "$\"New{Name}\"");
        script.Mappings.Add("NewMissingField", "$\"New{MissingField}\"");
        var memoryDestination = new MemoryDestination<ExpandoObject>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);
        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();
        // Assert
        Assert.Single(
            memoryDestination.Data,
            (dynamic row) =>
                row.NewId == 2 && row.NewName == "NewTest" && row.NewMissingField == null
        );
    }

    [Fact]
    public void ShouldFailTransformExpandoObjectWithMissingFieldOnSource()
    {
        // Arrange
        var memorySource = new MemorySource();
        memorySource.DataAsList.Add(CreateTestDataItem(1, "Test"));
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>();
        script.Mappings.Add("NewId", "Id + 1");
        script.Mappings.Add("NewName", "$\"New{Name}\"");
        script.Mappings.Add("NewMissingField", "$\"New{MissingField}\"");
        script.FailOnMissingField = true;
        var memoryDestination = new MemoryDestination<ExpandoObject>();
        var errorDestination = new MemoryDestination<ETLBoxError>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);
        script.LinkErrorTo(errorDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();
        errorDestination.Wait();
        // Assert
        Assert.Single(errorDestination.Data);
    }

    [Fact]
    public void ShouldTransformTypedObject()
    {
        // Arrange
        var memorySource = new MemorySource<MyRowType>();
        memorySource.DataAsList.Add(new MyRowType { Id = 1, Name = "Test" });
        var script = new ScriptedRowTransformation<MyRowType, MyRowType>();
        script.Mappings.Add("Id", "Id + 1");
        script.Mappings.Add("Name", "$\"New{Name}\"");
        var memoryDestination = new MemoryDestination<MyRowType>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);
        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();
        // Assert
        Assert.Single(memoryDestination.Data, row => row.Id == 2 && row.Name == "NewTest");
    }

    [Fact]
    public void ShouldNotFailTransformExpandoObjectWithEmptySource()
    {
        // Arrange
        var memorySource = new MemorySource();
        memorySource.DataAsList.Add(new ExpandoObject());
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>();
        script.FailOnMissingField = false;
        script.Mappings.Add("NewId", "Id + 1");
        script.Mappings.Add("NewName", "$\"New{Name}\"");
        script.Mappings.Add("NewMissingField", "$\"New{MissingField}\"");
        var memoryDestination = new MemoryDestination<ExpandoObject>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();
        // Assert
        Assert.Single(memoryDestination.Data);
    }

    [Fact]
    public void ShouldGetNewId()
    {
        // Arrange
        var memorySource = new MemorySource();
        memorySource.DataAsList.Add(CreateTestDataItem(1, "Test"));
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>
        {
            FailOnMissingField = true
        };
        script.Mappings.Add("NewId", "MassTransit.NewId.NextSequentialGuid()");
        script.Mappings.Add("Id", "Id + 1");

        var assemblyFileName = $"{typeof(MassTransit.NewId).Assembly.GetName().Name!}.dll";
        script.AdditionalAssemblyLocations = new[] { assemblyFileName };

        var memoryDestination = new MemoryDestination<ExpandoObject>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();
        // Assert
        Assert.Single(memoryDestination.Data);
        var obj = memoryDestination.Data.First() as IDictionary<string, object?>;
        Assert.True(obj["NewId"] is Guid);
    }

    [Fact]
    public void ShouldSerializeToJsonFromProperty()
    {
        // Arrange
        var data = new ExpandoObject() as IDictionary<string, object?>;
        data["Id"] = 1;
        data["Name"] = "test";

        var obj = new ExpandoObject() as IDictionary<string, object?>;
        obj["Data"] = data as ExpandoObject;

        var memorySource = new MemorySource();
        memorySource.DataAsList.Add((ExpandoObject)obj);

        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>
        {
            FailOnMissingField = true
        };
        script.Mappings.Add("Id", "Data.Id + 1");
        script.Mappings.Add("Json", "Newtonsoft.Json.JsonConvert.SerializeObject(Data)");

        var assemblyFileName = $"{typeof(Newtonsoft.Json.JsonConvert).Assembly.GetName().Name!}.dll";
        script.AdditionalAssemblyLocations = new[] { assemblyFileName };

        var memoryDestination = new MemoryDestination<ExpandoObject>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();
        // Assert
        Assert.Single(memoryDestination.Data);
        var res = memoryDestination.Data.First() as IDictionary<string, object?>;
        Assert.Equal(2, res["Id"]);
        Assert.Equal("{\"Id\":1,\"Name\":\"test\"}", res["Json"]);
    }

    private static ExpandoObject CreateTestDataItem(int id, string name)
    {
        var result = new ExpandoObject();
        result.TryAdd("Id", id);
        result.TryAdd("Name", name);
        return result;
    }

    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    public class MyRowType
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }
}
