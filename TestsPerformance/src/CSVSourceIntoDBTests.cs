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
using System.Dynamic;
using System.IO;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace ALE.ETLBoxTests.Performance
{
    [Collection("Performance")]
    public class CsvSourceIntoDBTests
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



        public CsvSourceIntoDBTests(PerformanceDatabaseFixture dbFixture, ITestOutputHelper output)
        {
            this.output = output;
        }


        private void ReCreateDestinationTable(IConnectionManager connection, string tableName)
        {
            var tableDef = new TableDefinition(tableName, BigDataCsvSource.DestTableCols);
            DropTableTask.DropIfExists(connection, tableName);
            tableDef.CreateTable(connection);
        }

        /*
         * X Rows with 1027 bytes per Row (1020 bytes data + 7 bytes for sql server)
         */
        [Theory, MemberData(nameof(SqlConnection), 100000, 1000, 0.5, 6.0),
            MemberData(nameof(MySqlConnection), 100000, 1000, 0.5, 0.0),
            MemberData(nameof(PostgresConnection), 100000, 1000, 0.5, 0.0),
            MemberData(nameof(SQLiteConnection), 100000, 1000, 0.5, 0.0)
            ]
        public void CompareFlowWithBulkInsert(IConnectionManager connection, int numberOfRows, int batchSize, double deviationGeneric, double deviationBulk)
        {
            //Arrange
            BigDataCsvSource.CreateCSVFileIfNeeded(numberOfRows);
            ReCreateDestinationTable(connection, "CsvDestinationNonGenericETLBox");
            ReCreateDestinationTable(connection, "CsvDestinationBulkInsert");
            ReCreateDestinationTable(connection, "CsvDestinationGenericETLBox");

            var sourceNonGeneric = new CsvSource(BigDataCsvSource.GetCompleteFilePath(numberOfRows));
            var destNonGeneric = new DbDestination(connection, "CsvDestinationNonGenericETLBox", batchSize);
            var sourceGeneric = new CsvSource<CSVData>(BigDataCsvSource.GetCompleteFilePath(numberOfRows));
            var destGeneric = new DbDestination<CSVData>(connection, "CsvDestinationGenericETLBox", batchSize);

            //Act
            var timeElapsedBulkInsert = GetBulkInsertTime(connection, numberOfRows);
            var timeElapsedETLBoxNonGeneric = GetETLBoxTime(numberOfRows, sourceNonGeneric, destNonGeneric);
            var timeElapsedETLBoxGeneric = GetETLBoxTime(numberOfRows, sourceGeneric, destGeneric);


            //Assert
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CsvDestinationNonGenericETLBox"));
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CsvDestinationGenericETLBox"));
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
            if (connection.GetType() == typeof(SqlConnectionManager) && 1==0)
            {
                result = BigDataHelper.LogExecutionTime($"Copying Csv into DB (non generic) with rows of data using BulkInsert",
                 () =>
                 {
                     SqlTask.ExecuteNonQuery(connection, "Insert with BulkInsert",
            $@"BULK INSERT [dbo].[CsvDestinationBulkInsert]
        FROM '{BigDataCsvSource.GetCompleteFilePath(numberOfRows)}'
        WITH ( FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n' );
        ");
                 });
                Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CsvDestinationBulkInsert"));
                output.WriteLine("Elapsed " + result.TotalSeconds + " seconds for bulk insert.");
            }

            return result ;
        }

        private TimeSpan GetETLBoxTime<T>(int numberOfRows, CsvSource<T> source, DbDestination<T> dest)
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


        [Theory, MemberData(nameof(SqlConnection), 10000000, 1000, 1.0,1.0)]
        public void WithExpandoToObjectTransformation(IConnectionManager connection, int numberOfRows, int batchSize, double deviationGeneric, double deviationBulk)
        {
            //Arrange
            BigDataCsvSource.CreateCSVFileIfNeeded(numberOfRows);
            ReCreateDestinationTable(connection, "CsvDestinationWithTransformation");

            var sourceExpando = new CsvSource(BigDataCsvSource.GetCompleteFilePath(numberOfRows));
            var trans = new RowTransformation<ExpandoObject, CSVData>(
               row =>
               {
                   dynamic r = row as ExpandoObject;
                   return new CSVData()
                   {
                       Col1 = r.Col1,
                       Col2 = r.Col2,
                       Col3 = r.Col3,
                       Col4 = r.Col4
                   };
                });
            var destGeneric = new DbDestination<CSVData>(connection, "CsvDestinationWithTransformation", batchSize);

            //Act
            sourceExpando.LinkTo(trans);
            trans.LinkTo(destGeneric);
            var timeElapsedETLBox = BigDataHelper.LogExecutionTime($"Copying Csv into DB (non generic) with {numberOfRows} rows of data using ETLBox",
                () =>
                {
                    sourceExpando.Execute();
                    destGeneric.Wait();
                }
            );
            output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds + " seconds for ETLBox (Expando to object transformation).");

            //Assert
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CsvDestinationWithTransformation"));
            //10.000.000 rows, batch size 10.000: ~8 min
            //10.000.000 rows, batch size  1.000: ~10 min 10 sec
        }

    }
}
