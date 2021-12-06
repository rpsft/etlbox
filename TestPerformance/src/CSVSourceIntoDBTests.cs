using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using Xunit;
using Xunit.Abstractions;

namespace ALE.ETLBoxTests.Performance
{
    [Collection("Performance")]
    public class CsvSourceIntoDBTests
    {
        private readonly ITestOutputHelper _output;


        public CsvSourceIntoDBTests(PerformanceDatabaseFixture dbFixture, ITestOutputHelper output)
        {
            _output = output;
            // DotMemoryUnitTestOutput.SetOutputMethod(_output.WriteLine);
        }

        public static IEnumerable<object[]> SqlConnection(int numberOfRows, int batchSize, double deviation) => new[]
        {
            new object[] { Config.SqlConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviation }
        };

        public static IEnumerable<object[]> SqlOdbcConnection(int numberOfRows, int batchSize, double deviation) =>
            new[]
            {
                new object[]
                    { Config.SqlOdbcConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviation }
            };

        public static IEnumerable<object[]> MySqlConnection(int numberOfRows, int batchSize, double deviation) => new[]
        {
            new object[] { Config.MySqlConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviation }
        };

        public static IEnumerable<object[]> PostgresConnection(int numberOfRows, int batchSize, double deviation) =>
            new[]
            {
                new object[]
                    { Config.PostgresConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviation }
            };

        public static IEnumerable<object[]> SQLiteConnection(int numberOfRows, int batchSize, double deviation) => new[]
        {
            new object[]
                { Config.SQLiteConnection.ConnectionManager("Performance"), numberOfRows, batchSize, deviation }
        };


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
        public void GenericAndDynamicAreNotTooDifferent(IConnectionManager connection, int numberOfRows, int batchSize,
            double deviation)
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

            var timeDifference = Math.Abs(timeElapsedETLBoxGeneric.TotalMilliseconds -
                                          timeElapsedETLBoxNonGeneric.TotalMilliseconds);

            var diffPercentage = timeDifference / Math.Min(timeElapsedETLBoxGeneric.TotalMilliseconds,
                timeElapsedETLBoxNonGeneric.TotalMilliseconds);
            
            Assert.InRange(diffPercentage, 0.0, deviation);
        }

        private TimeSpan GetETLBoxTime<T>(int numberOfRows, CsvSource<T> source, DbDestination<T> dest)
        {
            source.LinkTo(dest);
            var timeElapsedETLBox = BigDataHelper.LogExecutionTime(
                $"Copying Csv into DB (non generic) with {numberOfRows} rows of data using ETLBox",
                () =>
                {
                    source.Execute();
                    dest.Wait();
                }
            );
            if (typeof(T) == typeof(string[]))
                _output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds + " seconds for ETLBox (Non generic).");
            else
                _output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds + " seconds for ETLBox (Generic).");
            return timeElapsedETLBox;
        }


        [Theory, MemberData(nameof(SqlConnection), 1000000, 1000, 1.3)]
        public void CheckMemoryUsage(IConnectionManager connection, int numberOfRows, int batchSize, double deviation)
        {
            //Arrange
            BigDataCsvSource.CreateCSVFileIfNeeded(numberOfRows);
            ReCreateDestinationTable(connection, "CsvDestinationWithTransformation");

            var sourceExpando = new CsvSource(BigDataCsvSource.GetCompleteFilePath(numberOfRows));
            var trans = new RowTransformation<ExpandoObject, CSVData>(
                row =>
                {
                    dynamic r = row;
                    return new CSVData
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
            long memAfter;
            long memBefore = 0;
            // MemoryCheckPoint memoryCheckPointBefore;
            var startCheck = true;
            var count = 1;
            destGeneric.AfterBatchWrite = _ =>
            {
                if (count++ % 50 != 0) return;

                using var proc = Process.GetCurrentProcess();

                memAfter = proc.WorkingSet64;
                if (startCheck)
                {
                    memBefore = memAfter;
                    // memoryCheckPointBefore = dotMemory.Check();
                    startCheck = false;
                }
                Assert.InRange(memAfter, 0, memBefore + memBefore * deviation);
                // dotMemory.Check(memory =>
                // {
                //     Assert.InRange(memory.GetTrafficFrom(memoryCheckPointBefore).AllocatedMemory.SizeInBytes, 0,
                //         memBefore * deviation);
                // });
            };

            var timeElapsedETLBox = BigDataHelper.LogExecutionTime(
                $"Copying Csv into DB (non generic) with {numberOfRows} rows of data using ETLBox",
                () =>
                {
                    sourceExpando.Execute();
                    destGeneric.Wait();
                }
            );
            _output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds +
                              " seconds for ETLBox (Expando to object transformation).");

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
                yield return new CSVData
                {
                    Col1 = HashHelper.RandomString(255),
                    Col2 = HashHelper.RandomString(255),
                    Col3 = HashHelper.RandomString(255),
                    Col4 = HashHelper.RandomString(255)
                };
            }
        }

        [Theory, MemberData(nameof(SqlConnection), 1000000, 1000, 1.0)]
#pragma warning disable xUnit1026
        public void CheckMemoryUsageDbDestination(IConnectionManager connection, int numberOfRows, int batchSize, double _)
#pragma warning restore xUnit1026
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