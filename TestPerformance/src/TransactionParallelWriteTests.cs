using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
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
    public class TransactionParallelWriteTests
    {
        private readonly ITestOutputHelper output;

        public static IEnumerable<object[]> SqlConnection(int numberOfRows) => new[] {
            new object[] { (IConnectionManager)Config.SqlConnection.ConnectionManager("Performance") , numberOfRows},
        };

        public static IEnumerable<object[]> SqlOdbcConnection(int numberOfRows) => new[] {
            new object[] { (IConnectionManager)Config.SqlOdbcConnection.ConnectionManager("Performance") , numberOfRows},
        };

        public static IEnumerable<object[]> MySqlConnection(int numberOfRows) => new[] {
            new object[] { (IConnectionManager)Config.MySqlConnection.ConnectionManager("Performance"), numberOfRows }
        };

        public static IEnumerable<object[]> PostgresConnection(int numberOfRows) => new[] {
            new object[] { (IConnectionManager)Config.PostgresConnection.ConnectionManager("Performance"), numberOfRows}
        };

        public static IEnumerable<object[]> SQLiteConnection(int numberOfRows) => new[] {
            new object[] { (IConnectionManager)Config.SQLiteConnection.ConnectionManager("Performance"), numberOfRows }
        };



        public TransactionParallelWriteTests(PerformanceDatabaseFixture _dbFixture, ITestOutputHelper output)
        {
            this.output = output;
        }


        private void ReCreateDestinationTable(IConnectionManager connection, string tableName)
        {
            var tableDef = new TableDefinition(tableName, BigDataCsvSource.DestTableCols);
            DropTableTask.DropIfExists(connection, tableName);
            tableDef.CreateTable(connection);
        }

        [Theory, MemberData(nameof(SqlConnection), 1000000)
            ]
        public void WriteParallelWhileTransactionOpen(IConnectionManager connection, int numberOfRows)
        {
            //Arrange
            BigDataCsvSource.CreateCSVFileIfNeeded(numberOfRows);
            ReCreateDestinationTable(connection, "TransactionDestination1");
            ReCreateDestinationTable(connection, "TransactionDestination2");

            var source = new CsvSource(BigDataCsvSource.GetCompleteFilePath(numberOfRows));
            var dest1 = new DbDestination(connection, "TransactionDestination1");
            var dest2 = new DbDestination(connection, "TransactionDestination2");
            var multi = new Multicast();

            //Act & Assert
            Assert.ThrowsAny<Exception>(() =>
            {
                connection.BeginTransaction();
                source.LinkTo(multi);
                multi.LinkTo(dest1);
                multi.LinkTo(dest2);
                source.Execute();
                dest1.Wait();
                dest2.Wait();
            });
        }
    }
}
