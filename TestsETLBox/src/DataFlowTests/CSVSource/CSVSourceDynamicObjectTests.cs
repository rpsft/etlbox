using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using CsvHelper.Configuration.Attributes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvSourceDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvSourceDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleFlowWithDynamicObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CSVSourceDynamic");
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(SqlConnection, "CSVSourceDynamic");

            //Act
            CsvSource<ExpandoObject> source = new CsvSource<ExpandoObject>("res/CSVSource/TwoColumnsForDynamic.csv");
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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CSVSourceDynamicColsInSource");
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(SqlConnection, "CSVSourceDynamicColsInSource");

            //Act
            CsvSource<ExpandoObject> source = new CsvSource<ExpandoObject>("res/CSVSource/FourColumnsForDynamic.csv");
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
            CreateTableTask.Create(SqlConnection, "CSVSourceDynamicColsInDest",
                new List<TableColumn>() {
                    new TableColumn("Col2", "VARCHAR(100)",allowNulls:true),
                    new TableColumn("Id", "INT", allowNulls:false, isPrimaryKey:true, isIdentity:true),
                    new TableColumn("Col1", "INT",allowNulls:true),
                    new TableColumn("ColX", "INT",allowNulls:true),
            });
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(SqlConnection, "CSVSourceDynamicColsInDest");

            //Act
            CsvSource<ExpandoObject> source = new CsvSource<ExpandoObject>("res/CSVSource/TwoColumnsForDynamic.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, "CSVSourceDynamicColsInDest"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "CSVSourceDynamicColsInDest", $"Col1 = 1 AND Col2='Test1' AND Id > 0 AND ColX IS NULL"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "CSVSourceDynamicColsInDest", $"Col1 = 2 AND Col2='Test2' AND Id > 0 AND ColX IS NULL"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "CSVSourceDynamicColsInDest", $"Col1 = 3 AND Col2='Test3' AND Id > 0 AND ColX IS NULL"));

        }
    }
}
