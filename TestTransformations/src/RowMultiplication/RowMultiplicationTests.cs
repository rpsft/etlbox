using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowMultiplicationTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowMultiplicationTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        public class MyOtherRow
        {
            public int Col3 { get; set; }
        }

        [Fact]
        public void RandomDoubling()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("RowMultiplicationSource");
            source2Columns.InsertTestData();

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "RowMultiplicationSource");
            RowMultiplication<MySimpleRow> multiplication = new RowMultiplication<MySimpleRow>(
                row =>
                {
                    List<MySimpleRow> result = new List<MySimpleRow>();
                    for (int i = 0; i < row.Col1; i++)
                    {
                        result.Add(new MySimpleRow()
                        {
                            Col1 = row.Col1 + i,
                            Col2 = "Test" + (row.Col1 + i)
                        });
                    }
                    return result;
                });
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            source.LinkTo(multiplication);
            multiplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(dest.Data,
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3"),
                d => Assert.True(d.Col1 == 4 && d.Col2 == "Test4"),
                d => Assert.True(d.Col1 == 5 && d.Col2 == "Test5")
            );
        }

        [Fact]
        public void DifferentOutputType()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("RowMultiplicationSource");
            source2Columns.InsertTestData();

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "RowMultiplicationSource");
            RowMultiplication<MySimpleRow, MyOtherRow> multiplication = new RowMultiplication<MySimpleRow, MyOtherRow>(
                row =>
                {
                    List<MyOtherRow> result = new List<MyOtherRow>();
                    for (int i = 0; i <= row.Col1; i++)
                    {
                        result.Add(new MyOtherRow()
                        {
                            Col3 = i * row.Col1,
                        });
                    }
                    return result;
                });
            MemoryDestination<MyOtherRow> dest = new MemoryDestination<MyOtherRow>();

            //Act
            source.LinkTo(multiplication);
            multiplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(dest.Data,
                d => Assert.True(d.Col3 == 0),
                d => Assert.True(d.Col3 == 1),
                d => Assert.True(d.Col3 == 0),
                d => Assert.True(d.Col3 == 2),
                d => Assert.True(d.Col3 == 4),
                d => Assert.True(d.Col3 == 0),
                d => Assert.True(d.Col3 == 3),
                d => Assert.True(d.Col3 == 6),
                d => Assert.True(d.Col3 == 9)
            );
        }
    }
}
