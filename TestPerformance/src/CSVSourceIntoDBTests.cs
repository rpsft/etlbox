using ETLBox;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using Xunit;
using Xunit.Abstractions;

namespace ETLBoxTests.Performance
{
    [Collection("Performance")]
    public class CsvSourceIntoDBTests
    {
        private readonly ITestOutputHelper output;

        public static IEnumerable<object[]> SqlConnection(int numberOfRows, int batchSize, double deviation) => new[] {
            new object[] { (IConnectionManager)Config.SqlConnection.ConnectionManager("Performance") , numberOfRows, batchSize, deviation},
        };

        public static IEnumerable<object[]> SqlOdbcConnection(int numberOfRows, int batchSize, double deviation) => new[] {
            new object[] { (IConnectionManager)Config.SqlOdbcConnection.ConnectionManager("Performance") , numberOfRows, batchSize, deviation},
        };

        public static IEnumerable<object[]> MySqlConnection(int numberOfRows, int batchSize, double deviation) => new[] {
            new object[] { (IConnectionManager)Config.MySqlConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviation }
        };

        public static IEnumerable<object[]> PostgresConnection(int numberOfRows, int batchSize, double deviation) => new[] {
            new object[] { (IConnectionManager)Config.PostgresConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviation }
        };

        public static IEnumerable<object[]> SQLiteConnection(int numberOfRows, int batchSize, double deviation) => new[] {
            new object[] { (IConnectionManager)Config.SQLiteConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviation }
        };



        public CsvSourceIntoDBTests(PerformanceDatabaseFixture _dbFixture, ITestOutputHelper output)
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
        [Theory, MemberData(nameof(SqlConnection), 100000, 1000, 0.5),
            MemberData(nameof(MySqlConnection), 100000, 1000, 0.5),
            MemberData(nameof(PostgresConnection), 100000, 1000, 0.5),
            MemberData(nameof(SQLiteConnection), 100000, 1000, 0.5)
            ]
        public void CompareGenericAndDynamic(IConnectionManager connection, int numberOfRows, int batchSize, double deviation)
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
            var timeElapsedETLBoxNonGeneric = GetETLBoxTime(numberOfRows, sourceNonGeneric, destNonGeneric);
            var timeElapsedETLBoxGeneric = GetETLBoxTime(numberOfRows, sourceGeneric, destGeneric);

            //Assert
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CsvDestinationNonGenericETLBox"));
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "CsvDestinationGenericETLBox"));
            Assert.True(Math.Abs(timeElapsedETLBoxGeneric.TotalMilliseconds - timeElapsedETLBoxNonGeneric.TotalMilliseconds) <
                Math.Min(timeElapsedETLBoxGeneric.TotalMilliseconds, timeElapsedETLBoxNonGeneric.TotalMilliseconds) * deviation);
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
            if (typeof(T) == typeof(string[]))
                output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds + " seconds for ETLBox (Non generic).");
            else
                output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds + " seconds for ETLBox (Generic).");
            return timeElapsedETLBox;
        }


        [Theory, MemberData(nameof(SqlConnection), 10000000, 1000, 1.0)]
        public void CheckMemoryUsage(IConnectionManager connection, int numberOfRows, int batchSize, double deviation)
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
            sourceExpando.LinkTo(trans);
            trans.LinkTo(destGeneric);

            //Act
            long memAfter = 0;
            long memBefore = 0;
            bool startCheck = true;
            int count = 1;
            destGeneric.AfterBatchWrite = data =>
            {
                if (count++ % 50 == 0)
                {
                    using Process proc = Process.GetCurrentProcess();
                    memAfter = proc.WorkingSet64;
                    if (startCheck)
                    {
                        memBefore = memAfter;
                        startCheck = false;
                    }
                    Assert.True(memAfter < (memBefore + (memBefore * deviation)));
                }
            };

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

        IEnumerable<CSVData> GenerateWithYield(int numberOfRows)
        {
            var i = 0;
            while (i < numberOfRows)
            {
                i++;
                yield return new CSVData()
                {
                    Col1 = TestHashHelper.RandomString(255),
                    Col2 = TestHashHelper.RandomString(255),
                    Col3 = TestHashHelper.RandomString(255),
                    Col4 = TestHashHelper.RandomString(255)
                };
            }
        }

        [Theory, MemberData(nameof(SqlConnection), 1000000, 1000, 1.0)]
        public void CheckMemoryUsageDbDestination(IConnectionManager connection, int numberOfRows, int batchSize)
        {
            //Arrange
            ReCreateDestinationTable(connection, "MemoryDestination");

            var source = new MemorySource<CSVData>
            {
                Data = GenerateWithYield(numberOfRows)
            };
            var dest = new DbDestination<CSVData>(connection, "MemoryDestination", batchSize);

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(numberOfRows, RowCountTask.Count(connection, "MemoryDestination"));

        }

    }
}
