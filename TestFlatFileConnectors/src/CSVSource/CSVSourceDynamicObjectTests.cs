using System.Collections.Generic;
using System.Dynamic;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.CSVSource
{
    [Collection("DataFlow")]
    public class CsvSourceDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleFlowWithDynamicObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CsvSourceDynamic");
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "CsvSourceDynamic"
            );

            //Act
            CsvSource<ExpandoObject> source = new CsvSource<ExpandoObject>(
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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "CsvSourceDynamicColsInSource"
            );
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "CsvSourceDynamicColsInSource"
            );

            //Act
            CsvSource<ExpandoObject> source = new CsvSource<ExpandoObject>(
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
                    new("ColX", "INT", allowNulls: true),
                }
            );
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "CsvSourceDynamicColsInDest"
            );

            //Act
            CsvSource<ExpandoObject> source = new CsvSource<ExpandoObject>(
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
