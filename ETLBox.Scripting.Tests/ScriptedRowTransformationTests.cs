using System.Dynamic;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Scripting;
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
        memorySource.Execute();
        memoryDestination.Wait();
        // Assert
        Assert.Collection(
            memoryDestination.Data,
            (dynamic row) =>
            {
                Assert.Equal(2, row.NewId);
                Assert.Equal("NewTest", row.NewName);
            }
        );
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
        memorySource.Execute();
        memoryDestination.Wait();
        // Assert
        Assert.Collection(
            memoryDestination.Data,
            row =>
            {
                Assert.Equal(2, row.Id);
                Assert.Equal("NewTest", row.Name);
            }
        );
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
