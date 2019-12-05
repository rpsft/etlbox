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
using System.IO;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace ALE.ETLBoxTests.Performance
{
    [Collection("Performance")]
    public class CSVSourceIntoDBTests : IDisposable
    {
        private readonly ITestOutputHelper output;

        public static IEnumerable<object[]> SqlConnection(int numberOfRows, int batchSize, double deviationGeneric, double deviationBulk) => new[] {
            new object[] { (IConnectionManager)Config.SqlConnection.ConnectionManager("Performance") , numberOfRows, batchSize, deviationGeneric, deviationBulk},
        };

        public static IEnumerable<object[]> SqlOdbcConnection(int numberOfRows, int batchSize, double deviationGeneric, double deviationBulk) => new[] {
            new object[] { (IConnectionManager)Config.SqlOdbcConnection.ConnectionManager("Performance") , numberOfRows, batchSize, deviationGeneric, deviationBulk},
        };

        public static IEnumerable<object[]> MySqlConnection(int numberOfRows, int batchSize, double deviationGeneric, double deviationBulk) => new[] {
            new object[] { (IConnectionManager)Config.MySqlConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviationGeneric, deviationBulk }
        };

        public static IEnumerable<object[]> PostgresConnection(int numberOfRows, int batchSize, double deviationGeneric, double deviationBulk) => new[] {
            new object[] { (IConnectionManager)Config.PostgresConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviationGeneric, deviationBulk }
        };

        public static IEnumerable<object[]> SQLiteConnection(int numberOfRows, int batchSize, double deviationGeneric, double deviationBulk) => new[] {
            new object[] { (IConnectionManager)Config.SQLiteConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviationGeneric, deviationBulk }
        };

        static List<TableColumn> DestTableCols { get; } = new List<TableColumn>() {
                new TableColumn("Col1", "CHAR(255)", allowNulls: false),
                new TableColumn("Col2", "CHAR(255)", allowNulls: false),
                new TableColumn("Col3", "CHAR(255)", allowNulls: false),
                new TableColumn("Col4", "CHAR(255)", allowNulls: true),
            };

        public CSVSourceIntoDBTests(PerformanceDatabaseFixture dbFixture, ITestOutputHelper output)
        {
            this.output = output;
        }

        public void Dispose()
        {
        }

        internal static void CreateCSVFileIfNeeded(int numberOfRows)
        {
            if (!File.Exists(GetCompleteFilePath(numberOfRows)))
            {
                BigDataHelper bigData = new BigDataHelper()
                {
                    FileName = GetCompleteFilePath(numberOfRows),
                    NumberOfRows = numberOfRows,
                    TableDefinition = new TableDefinition("CSV", DestTableCols)
                };
                bigData.CreateBigDataCSV();
            }
        }

        static string CSVFolderName = "res/Csv";
        internal static string GetCompleteFilePath(int numberOfRows) =>
            Path.GetFullPath(Path.Combine(CSVFolderName, "TestData" + numberOfRows + ".csv"));

        private void ReCreateDestinationTable(IConnectionManager connection, string tableName)
        {
            var tableDef = new TableDefinition(tableName, DestTableCols);
            DropTableTask.DropIfExists(connection, tableName);
            tableDef.CreateTable(connection);
        }

        /*
         * X Rows with 1027 bytes per Row (1020 bytes data + 7 bytes for sql server)
         */
        [Theory, MemberData(nameof(SqlConnection), 10000, 1000,0.5, 6.0),
            MemberData(nameof(MySqlConnection), 10000, 1000, 0.5,0.0),
            MemberData(nameof(PostgresConnection), 10000, 1000, 0.5, 0.0),
            MemberData(nameof(SQLiteConnection), 10000, 1000, 0.5,0.0)]
        public void CompareFlowWithBulkInsert(IConnectionManager connection, int numberOfRows, int batchSize, double deviationGeneric, double deviationBulk)
        {
            //Arrange
            CreateCSVFileIfNeeded(numberOfRows);
            ReCreateDestinationTable(connection, "CSVDestinationNonGenericETLBox");
            ReCreateDestinationTable(connection, "CSVDestinationBulkInsert");
            ReCreateDestinationTable(connection, "CSVDestinationGenericETLBox");

            var sourceNonGeneric = new CSVSource(GetCompleteFilePath(numberOfRows));
            var destNonGeneric = new DBDestination(connection, "CSVDestinationNonGenericETLBox", batchSize);
            var sourceGeneric = new CSVSource<CSVData>(GetCompleteFilePath(numberOfRows));
            var destGeneric = new DBDestination<CSVData>(connection, "CSVDestinationGenericETLBox", batchSize);


            //Act
            var timeElapsedBulkInsert = GetBulkInsertTime(connection, numberOfRows);
            var timeElapsedETLBoxNonGeneric = GetETLBoxTime(numberOfRows, sourceNonGeneric, destNonGeneric);
            var timeElapsedETLBoxGeneric = GetETLBoxTime(numberOfRows, sourceGeneric, destGeneric);


            //Assert
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CSVDestinationNonGenericETLBox"));
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CSVDestinationGenericETLBox"));
            Assert.True(Math.Abs(timeElapsedETLBoxGeneric.TotalMilliseconds- timeElapsedETLBoxNonGeneric.TotalMilliseconds) <
                Math.Min(timeElapsedETLBoxGeneric.TotalMilliseconds, timeElapsedETLBoxNonGeneric.TotalMilliseconds) * deviationGeneric );
            if (timeElapsedBulkInsert.TotalMilliseconds > 0)
            {
                Assert.True(timeElapsedBulkInsert < timeElapsedETLBoxNonGeneric);
                Assert.True(timeElapsedBulkInsert.TotalMilliseconds * (deviationBulk + 1) > timeElapsedETLBoxNonGeneric.TotalMilliseconds);
            }
        }

        private TimeSpan GetBulkInsertTime(IConnectionManager connection, int numberOfRows)
        {
            TimeSpan result = TimeSpan.FromMilliseconds(0);
            if (connection.GetType() == typeof(SqlConnectionManager))
            {
                result = BigDataHelper.LogExecutionTime($"Copying Csv into DB (non generic) with rows of data using BulkInsert",
                 () =>
                 {
                     SqlTask.ExecuteNonQuery(connection, "Insert with BulkInsert",
            $@"BULK INSERT [dbo].[CSVDestinationBulkInsert]
        FROM '{GetCompleteFilePath(numberOfRows)}'
        WITH ( FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n' );
        ");
                 });
                Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CSVDestinationBulkInsert"));
                output.WriteLine("Elapsed " + result.TotalSeconds + " seconds for bulk insert.");
            }

            return result ;
        }

        private TimeSpan GetETLBoxTime<T>(int numberOfRows, CSVSource<T> source, DBDestination<T> dest)
        {
            source.LinkTo(dest);
            var timeElapsedETLBox = BigDataHelper.LogExecutionTime($"Copying Csv into DB (non generic) with {numberOfRows} rows of data using ETLBox",
                () =>
                {
                    source.Execute();
                    dest.Wait();
                }
            );
            if(typeof(T) == typeof(string[]))
                output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds + " seconds for ETLBox (Non generic).");
            else
                output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds + " seconds for ETLBox (Generic).");
            return timeElapsedETLBox;
        }

        public class CSVData
        {
            public string Col1 { get; set; }
            public string Col2 { get; set; }
            public string Col3 { get; set; }
            public string Col4 { get; set; }
        }
    }
}
