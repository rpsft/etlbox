using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.BigData
{
    [Collection("Big Data")]
    public class CSVSourceIntoDBTests : IDisposable
    {
        public static IEnumerable<object[]> Connections(int value1, int value2) => new[] {
                    //new object[] { (IConnectionManager)Config.SqlConnection.ConnectionManager("BigData") , value1, value2},
                    new object[] { (IConnectionManager)Config.SqlOdbcConnection.ConnectionManager("BigData") , value1, value2}
        };

        public CSVSourceIntoDBTests(BigDataDatabaseFixture dbFixture)
        {

        }
        public void Dispose()
        {

        }

        private TableDefinition CreateDestinationTable(IConnectionManager connection, string tableName)
        {
            DropTableTask.Drop(connection, tableName);
            TableDefinition stagingTable = new TableDefinition(tableName, new List<TableColumn>() {
                new TableColumn("Col1", "nchar(1000)", allowNulls: false),
                new TableColumn("Col2", "nchar(1000)", allowNulls: false),
                new TableColumn("Col3", "nchar(1000)", allowNulls: false),
                new TableColumn("Col4", "nchar(1000)", allowNulls: true),
            });
            stagingTable.CreateTable(connection);
            return stagingTable;
        }

        private void CreateCSVFile(string fileName, int numberOfRows, TableDefinition destinationTable)
        {
            BigDataHelper bigData = new BigDataHelper()
            {
                FileName = fileName,
                NumberOfRows = numberOfRows,
                TableDefinition = destinationTable
            };
            BigDataHelper.LogExecutionTime($"Create .csv file {fileName} with {numberOfRows} Rows",
                () => bigData.CreateBigDataCSV()
            );
        }
        /*
         * Table without key columns (HEAP)
         * X Rows with 8007 bytes per Row (8000 bytes data + 7 bytes for sql server)
         */
        [Theory,
            //MemberData(nameof(Connections), 3, 3)]
            MemberData(nameof(Connections), 200000, 1000)]
        public void UsingNonGeneric(IConnectionManager connection, int numberOfRows, int batchSize)
        {
            //Arrange
            TableDefinition destinationTable = CreateDestinationTable(connection, "CSVDestination");
            CreateCSVFile("res/Csv/TestData.csv", numberOfRows, destinationTable);

            var source = new CSVSource("res/Csv/TestData.csv");
            var dest = new DBDestination(batchSize)
            {
                DestinationTableDefinition = destinationTable,
                ConnectionManager = connection
            };
            source.LinkTo(dest);

            //Act
            BigDataHelper.LogExecutionTime($"Copying Csv into DB (non generic) wswith {numberOfRows} rows of data",
                () =>
                {
                    source.Execute();
                    dest.Wait();
                }
            );

            //Assert
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CSVDestination", RowCountOptions.QuickQueryMode));
        }

        public class CSVData
        {
            public string Col1 { get; set; }
            public string Col2 { get; set; }
            public string Col3 { get; set; }
            public string Col4 { get; set; }
        }

        [Theory]
        [MemberData(nameof(Connections), 1000, 100)]
        //[InlineData("res/Csv/TestData.csv", 100000, 1000)]
        //[InlineData("res/Csv/TestData.csv", 1000000, 10000)]
        //[InlineData("res/Csv/TestData.csv", 10000000, 100000)]
        public void UsingPoco(IConnectionManager connection, int numberOfRows, int batchSize)
        {
            //Arrange
            TableDefinition destinationTable = CreateDestinationTable(connection, "CSVDestination");
            CreateCSVFile("res/Csv/TestData.csv", numberOfRows, destinationTable);

            var source = new CSVSource<CSVData>("res/Csv/TestData.csv");
            var dest = new DBDestination<CSVData>(batchSize)
            {
                DestinationTableDefinition = destinationTable,
                ConnectionManager = connection
            };
            source.LinkTo(dest);

            //Act
            BigDataHelper.LogExecutionTime($"Copying Csv into DB (POCO) wswith {numberOfRows} rows of data",
                () =>
                {
                    source.Execute();
                    dest.Wait();
                }
            );

            //Assert
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CSVDestination", RowCountOptions.QuickQueryMode));
        }



    }
}
