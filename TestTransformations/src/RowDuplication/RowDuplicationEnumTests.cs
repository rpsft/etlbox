using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowDuplicationEnumTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowDuplicationEnumTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyEnumRow
        {
            public EnumType EnumCol { get; set; }
        }

        public enum EnumType
        {
            Value1 = 1,
            Value2 = 2
        }

        [Fact]
        public void NoParameter()
        {
            //Arrange
            MemorySource<MyEnumRow> source = new MemorySource<MyEnumRow>();
            source.DataAsList.Add(new MyEnumRow() { EnumCol = EnumType.Value2 });
            RowDuplication<MyEnumRow> duplication = new RowDuplication<MyEnumRow>();
            MemoryDestination<MyEnumRow> dest = new MemoryDestination<MyEnumRow>();

            //Act
            source.LinkTo(duplication);
            duplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<MyEnumRow>(dest.Data,
                d => Assert.True(d.EnumCol == EnumType.Value2),
                d => Assert.True(d.EnumCol == EnumType.Value2)
            );
        }
    }
}
