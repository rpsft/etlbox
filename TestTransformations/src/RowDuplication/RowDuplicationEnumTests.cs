using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests;

[Collection("DataFlow")]
public class RowDuplicationEnumTests
{
    public enum EnumType
    {
        Value1 = 1,
        Value2 = 2
    }

    public RowDuplicationEnumTests(DataFlowDatabaseFixture dbFixture)
    {
    }

    public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

    [Fact]
    public void NoParameter()
    {
        //Arrange
        var source = new MemorySource<MyEnumRow>();
        source.DataAsList.Add(new MyEnumRow { EnumCol = EnumType.Value2 });
        var duplication = new RowDuplication<MyEnumRow>();
        var dest = new MemoryDestination<MyEnumRow>();

        //Act
        source.LinkTo(duplication);
        duplication.LinkTo(dest);
        source.Execute();
        dest.Wait();

        //Assert
        Assert.Collection(dest.Data,
            d => Assert.True(d.EnumCol == EnumType.Value2),
            d => Assert.True(d.EnumCol == EnumType.Value2)
        );
    }

    public class MyEnumRow
    {
        public EnumType EnumCol { get; set; }
    }
}