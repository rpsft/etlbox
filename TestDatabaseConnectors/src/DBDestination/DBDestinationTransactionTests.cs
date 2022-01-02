using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationTransactionTests : IClassFixture<DataFlowDatabaseFixture>
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public DbDestinationTransactionTests(DataFlowDatabaseFixture _)
        {
        }

        public class MySimpleRow
        {
            public string Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void ErrorInBatch(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "TransactionDest");
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = "1", Col2 = "Test1"},
                new MySimpleRow() { Col1 = "2", Col2 = "Test2"},
                new MySimpleRow() { Col1 = null, Col2 = "Test3"},
            };
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(connection, "TransactionDest", batchSize: 2);

            //Act & Assert
            source.LinkTo(dest);

            Assert.ThrowsAny<Exception>(() =>
            {
                source.Execute();
                dest.Wait();
            });

            //Assert
            Assert.Equal(2, RowCountTask.Count(connection, "TransactionDest"));
            Assert.True(dest.BulkInsertConnectionManager.State == null);
            Assert.True(connection.State == null);
        }

        [Theory, MemberData(nameof(Connections))]
        public void CloseConnectionDuringTransaction(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "TransactionSource");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "TransactionDest");
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(connection, "TransactionSource");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(connection, "TransactionDest", batchSize: 2);

            //Act & Assert
            connection.BeginTransaction();
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.Equal(3, RowCountTask.Count(connection, "TransactionDest"));
            connection.Close();
            Assert.Equal(0, RowCountTask.Count(connection, "TransactionDest"));

            //Assert Connections are closed
            Assert.True(dest.BulkInsertConnectionManager.State == null);
            Assert.True(connection.State == null);
        }

        [Theory, MemberData(nameof(Connections))]
        public void CommitTransaction(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "TransactionSource");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "TransactionDest");
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(connection, "TransactionSource");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(connection, "TransactionDest", batchSize: 2);

            //Act & Assert
            connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            //Assert
            if (connection.GetType() == typeof(SqlConnectionManager))
                Assert.Equal(3, RowCountTask.Count(connection.Clone(), "TransactionDest", RowCountOptions.NoLock));
            connection.CommitTransaction();
            Assert.Equal(3, RowCountTask.Count(connection, "TransactionDest"));
            Assert.Equal(3, RowCountTask.Count(connection.Clone(), "TransactionDest"));

            //Assert Connections are closed
            Assert.True(dest.BulkInsertConnectionManager.State == null);
            Assert.True(connection.State == null);
        }

        [Theory, MemberData(nameof(Connections))]
        public void RollbackTransaction(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "TransactionSource");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "TransactionDest");
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(connection, "TransactionSource");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(connection, "TransactionDest", batchSize: 2);

            //Act & Assert
            connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();
            Assert.Equal(3, RowCountTask.Count(connection, "TransactionDest"));
            connection.RollbackTransaction();
            Assert.Equal(0, RowCountTask.Count(connection, "TransactionDest"));

            //Assert Connections are closed
            Assert.True(dest.BulkInsertConnectionManager.State == null);
            Assert.True(connection.State == null);
        }

        [Theory, MemberData(nameof(Connections))]
        public void LeaveOpen(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "TransactionSource");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "TransactionDest");
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(connection, "TransactionSource");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(connection, "TransactionDest", batchSize: 2);

            //Act
            connection.LeaveOpen = true;
            connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            connection.CommitTransaction();

            //Assert
            Assert.Equal(connection, dest.BulkInsertConnectionManager);

            //Assert Connections are closed
            Assert.True(dest.BulkInsertConnectionManager.State == System.Data.ConnectionState.Open);
            Assert.True(connection.State == System.Data.ConnectionState.Open);

            Assert.Equal(3, RowCountTask.Count(connection, "TransactionDest"));


        }
    }
}
