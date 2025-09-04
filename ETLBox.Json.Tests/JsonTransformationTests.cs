using System.Dynamic;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using Xunit;

namespace ETLBox.Json.Tests;

[Collection("Transformations")]
public class JsonTransformationTests
{
    [Fact]
    public async Task DestinationJsonTransformationTest()
    {
        //Arrange
        ExpandoObject[] objSet =
        {
            CreateObject(@"{ ""Data"": { ""Id"": 1, ""Name"": ""Test1"" } }"),
            CreateObject(@"{ ""Data"": { ""Id"": 2, ""Name"": ""Test2"" } }"),
            CreateObject(@"{ ""Data"": { ""Id"": 3, ""Name"": ""Test3"" } }"),
        };

        var source = new MemorySource<ExpandoObject>(objSet);

        //Act
        var trans = new JsonTransformation
        {
            Mappings =
            {
                ["Col1"] = new JsonTransformation.Mapping("data", "$.Data.Id"),
                ["Col2"] = new JsonTransformation.Mapping("data", "$.Data.Name"),
            },
        };

        var dest = new MemoryDestination<ExpandoObject>();
        source.LinkTo(trans);
        trans.LinkTo(dest);
        await source.ExecuteAsync(CancellationToken.None);
        await dest.Completion.ConfigureAwait(true);

        //Assert
        Assert.Equal(3, dest.Data.Count);
        Assert.Collection(
            dest.Data,
            (dynamic d) =>
            {
                Assert.Equal(1, d.Col1);
                Assert.Equal("Test1", d.Col2);
            },
            d =>
            {
                Assert.Equal(2, d.Col1);
                Assert.Equal("Test2", d.Col2);
            },
            d =>
            {
                Assert.Equal(3, d.Col1);
                Assert.Equal("Test3", d.Col2);
            }
        );
    }

    [Fact]
    public async Task JsonTransformation_ShouldGetAnObjectFromSourceField()
    {
        //Arrange
        dynamic obj = new ExpandoObject();
        obj.EventId = Guid.NewGuid();

        var source = new MemorySource<ExpandoObject>(new[] { (ExpandoObject)obj });

        //Act
        var trans = new JsonTransformation
        {
            Mappings = { ["Col1"] = new JsonTransformation.Mapping("EventId", null!) },
        };

        var dest = new MemoryDestination<ExpandoObject>();
        source.LinkTo(trans);
        trans.LinkTo(dest);
        await source.ExecuteAsync(CancellationToken.None);
        await dest.Completion.ConfigureAwait(true);

        //Assert
        Assert.Single(dest.Data);
        var res = dest.Data.First() as IDictionary<string, object?>;
        Assert.Equal(obj.EventId, res["Col1"]);
    }

    private static ExpandoObject CreateObject(string v)
    {
        dynamic obj = new ExpandoObject();
        obj.data = v;
        return (ExpandoObject)obj;
    }
}
