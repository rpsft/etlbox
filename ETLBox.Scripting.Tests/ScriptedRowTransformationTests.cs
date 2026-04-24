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
    public void ShouldNotFailWhenFailOnMissingFieldIsFalseAndNestedPayloadChangedToEmpty()
    {
        // Arrange
        var itemWithPayload = new ExpandoObject() as IDictionary<string, object?>;
        itemWithPayload["meta"] = new ExpandoObject();
        itemWithPayload["payload"] = new ExpandoObject();
        var payload = (IDictionary<string, object?>)itemWithPayload["payload"]!;
        payload["ID"] = "7d51954d-5a06-4226-b7a3-defcdf0246a4";

        var itemWithEmptyPayload = new ExpandoObject() as IDictionary<string, object?>;
        itemWithEmptyPayload["meta"] = new ExpandoObject();
        itemWithEmptyPayload["payload"] = new ExpandoObject();

        var memorySource = new MemorySource();
        memorySource.DataAsList.Add((ExpandoObject)itemWithPayload);
        memorySource.DataAsList.Add((ExpandoObject)itemWithEmptyPayload);

        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>
        {
            FailOnMissingField = false,
        };
        script.Mappings.Add("ResultId", "payload.ID");

        var memoryDestination = new MemoryDestination<ExpandoObject>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();

        // Assert
        Assert.Equal(2, memoryDestination.Data.Count);
        var list = memoryDestination.Data.ToList();
        var first = (IDictionary<string, object?>)list[0];
        var second = (IDictionary<string, object?>)list[1];
        Assert.Equal("7d51954d-5a06-4226-b7a3-defcdf0246a4", first["ResultId"]);
        Assert.Null(second["ResultId"]);
    }

    [Fact]
    public void ShouldTransformExpandoObjectWithMissingFieldOnSource()
    {
        // Arrange
        var memorySource = new MemorySource();
        memorySource.DataAsList.Add(CreateTestDataItem(1, "Test"));
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>
        {
            FailOnMissingField = false,
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
        memorySource.DataAsList.Add([]);
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>
        {
            FailOnMissingField = false,
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
            FailOnMissingField = true,
        };
        script.Mappings.Add("NewId", "MassTransit.NewId.NextSequentialGuid()");
        script.Mappings.Add("Id", "Id + 1");

        var assemblyName = "Files/NewId.dll";
        script.AdditionalAssemblyNames = [assemblyName];

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
            FailOnMissingField = true,
        };
        script.Mappings.Add("Id", "Data.Id + 1");
        script.Mappings.Add("Json", "Newtonsoft.Json.JsonConvert.SerializeObject(Data)");

        script.AdditionalAssemblyNames = ["Newtonsoft.Json.dll"];

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

    [Fact]
    public void PassThrough_Dynamic_PreservesInputFieldsNotInMappings()
    {
        // Arrange
        var memorySource = new MemorySource();
        memorySource.DataAsList.Add(CreateTestDataItem(1, "Test"));
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>
        {
            PassThrough = true,
        };
        script.Mappings.Add("NewId", "Id + 1");
        var memoryDestination = new MemoryDestination<ExpandoObject>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();

        // Assert — both original fields and the new mapped field are present
        Assert.Single(memoryDestination.Data);
        var row = (IDictionary<string, object?>)memoryDestination.Data.First();
        Assert.Equal(1, row["Id"]);
        Assert.Equal("Test", row["Name"]);
        Assert.Equal(2, row["NewId"]);
    }

    [Fact]
    public void PassThrough_Dynamic_MappingOverridesExistingField()
    {
        // Arrange
        var memorySource = new MemorySource();
        memorySource.DataAsList.Add(CreateTestDataItem(1, "Test"));
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>
        {
            PassThrough = true,
        };
        script.Mappings.Add("Id", "Id + 100");
        var memoryDestination = new MemoryDestination<ExpandoObject>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();

        // Assert — Id is overridden by mapping, Name is preserved from pass-through
        Assert.Single(memoryDestination.Data);
        var row = (IDictionary<string, object?>)memoryDestination.Data.First();
        Assert.Equal(101, row["Id"]);
        Assert.Equal("Test", row["Name"]);
    }

    [Fact]
    public void PassThrough_False_Dynamic_OutputContainsOnlyMappedFields()
    {
        // Arrange — PassThrough defaults to false
        var memorySource = new MemorySource();
        memorySource.DataAsList.Add(CreateTestDataItem(1, "Test"));
        var script = new ScriptedRowTransformation<ExpandoObject, ExpandoObject>();
        script.Mappings.Add("NewId", "Id + 1");
        var memoryDestination = new MemoryDestination<ExpandoObject>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();

        // Assert — only the mapped field is present; originals are not passed through
        Assert.Single(memoryDestination.Data);
        var row = (IDictionary<string, object?>)memoryDestination.Data.First();
        Assert.True(row.ContainsKey("NewId"));
        Assert.False(row.ContainsKey("Id"));
        Assert.False(row.ContainsKey("Name"));
    }

    [Fact]
    public void PassThrough_Typed_PreservesInputPropertiesNotInMappings()
    {
        // Arrange
        var memorySource = new MemorySource<MyRowType>();
        memorySource.DataAsList.Add(new MyRowType { Id = 5, Name = "Alice" });
        var script = new ScriptedRowTransformation<MyRowType, MyRowType> { PassThrough = true };
        script.Mappings.Add("Id", "Id * 2");
        var memoryDestination = new MemoryDestination<MyRowType>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();

        // Assert — Name is preserved from pass-through; Id is overridden by the mapping
        Assert.Single(memoryDestination.Data);
        var row = memoryDestination.Data.First();
        Assert.Equal(10, row.Id);
        Assert.Equal("Alice", row.Name);
    }

    [Fact]
    public void PassThrough_False_Typed_PropertiesNotInMappingsAreDefault()
    {
        // Arrange
        var memorySource = new MemorySource<MyRowType>();
        memorySource.DataAsList.Add(new MyRowType { Id = 5, Name = "Alice" });
        var script = new ScriptedRowTransformation<MyRowType, MyRowType>();
        script.Mappings.Add("Id", "Id * 2");
        var memoryDestination = new MemoryDestination<MyRowType>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();

        // Assert — Name is absent from Mappings and not passed through, so it stays at default (null)
        Assert.Single(memoryDestination.Data);
        var row = memoryDestination.Data.First();
        Assert.Equal(10, row.Id);
        Assert.Null(row.Name);
    }

    [Fact]
    public void PassThrough_Typed_DifferentInputOutputTypes_DoesNotCopyProperties()
    {
        // Arrange — PassThrough only copies when TInput == TOutput (by design)
        var memorySource = new MemorySource<MyRowType>();
        memorySource.DataAsList.Add(new MyRowType { Id = 3, Name = "Bob" });
        var script = new ScriptedRowTransformation<MyRowType, MyOtherRowType>
        {
            PassThrough = true,
        };
        script.Mappings.Add("Value", "Id * 10");
        var memoryDestination = new MemoryDestination<MyOtherRowType>();
        memorySource.LinkTo(script);
        script.LinkTo(memoryDestination);

        // Act
        memorySource.Execute(CancellationToken.None);
        memoryDestination.Wait();

        // Assert — only mapped field is set; no cross-type copy occurs
        Assert.Single(memoryDestination.Data);
        var row = memoryDestination.Data.First();
        Assert.Equal(30, row.Value);
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

    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    public class MyOtherRowType
    {
        public int Value { get; set; }
    }
}
