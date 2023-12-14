using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestFlatFileConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestFlatFileConnectors.src.CsvSource
{
    public sealed class CsvSourceDynamicObjectTests : FlatFileConnectorsTestBase
    {
        public CsvSourceDynamicObjectTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SimpleFlowWithDynamicObject()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture("CsvSourceDynamic");
            var dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "CsvSourceDynamic"
            );

            //Act
            var source = new CsvSource<ExpandoObject>(
                "res/CsvSource/TwoColumnsForDynamic.csv"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void MoreColumnsInSource()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "CsvSourceDynamicColsInSource"
            );
            var dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "CsvSourceDynamicColsInSource"
            );

            //Act
            var source = new CsvSource<ExpandoObject>(
                "res/CsvSource/FourColumnsForDynamic.csv"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void MoreColumnsInDestination()
        {
            //Arrange
            CreateTableTask.Create(
                SqlConnection,
                "CsvSourceDynamicColsInDest",
                new List<TableColumn>
                {
                    new("Col2", "VARCHAR(100)", allowNulls: true),
                    new("Id", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
                    new("Col1", "INT", allowNulls: true),
                    new("ColX", "INT", allowNulls: true)
                }
            );
            var dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "CsvSourceDynamicColsInDest"
            );

            //Act
            var source = new CsvSource<ExpandoObject>(
                "res/CsvSource/TwoColumnsForDynamic.csv"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, "CsvSourceDynamicColsInDest"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "CsvSourceDynamicColsInDest",
                    "Col1 = 1 AND Col2='Test1' AND Id > 0 AND ColX IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "CsvSourceDynamicColsInDest",
                    "Col1 = 2 AND Col2='Test2' AND Id > 0 AND ColX IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "CsvSourceDynamicColsInDest",
                    "Col1 = 3 AND Col2='Test3' AND Id > 0 AND ColX IS NULL"
                )
            );
        }
    }
}
