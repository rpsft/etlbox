using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationTransactionTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public DbDestinationTransactionTests(DataFlowDatabaseFixture dbFixture)
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

        [Theory, MemberData(nameof(Connections))]
        public void TwoTransactionsAndParallelWriting(IConnectionManager connection)
        {
            if (connection.ConnectionManagerType == ConnectionManagerType.SQLite) return;
            //Arrange
            var concopy = connection.Clone();
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "TransactionSourceParallelWrite");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c1 = new TwoColumnsTableFixture(connection, "TransactionDest1");
            TwoColumnsTableFixture d2c2 = new TwoColumnsTableFixture(connection, "TransactionDest2");
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(connection, "TransactionSourceParallelWrite");
            DbDestination<MySimpleRow> dest1 = new DbDestination<MySimpleRow>(connection, "TransactionDest1", batchSize: 2);
            DbDestination<MySimpleRow> dest2 = new DbDestination<MySimpleRow>(concopy, "TransactionDest2", batchSize: 2);
            Multicast<MySimpleRow> multicast = new Multicast<MySimpleRow>();

            //Act & Assert
            connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            concopy.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            source.LinkTo(multicast);
            multicast.LinkTo(dest1);
            multicast.LinkTo(dest2);

            source.Execute();
            dest1.Wait();
            dest2.Wait();
            connection.CommitTransaction();
            concopy.CommitTransaction();

            Assert.Equal(3, RowCountTask.Count(connection, "TransactionDest1"));
            Assert.Equal(3, RowCountTask.Count(connection, "TransactionDest2"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void OneTransactionAndParallelWriting(IConnectionManager connection)
        {
            if (connection.ConnectionManagerType == ConnectionManagerType.SQLite) return;
            if (connection.ConnectionManagerType == ConnectionManagerType.Oracle) return;

            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "TransactionSourceParallelWrite");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c1 = new TwoColumnsTableFixture(connection, "TransactionDest1");
            TwoColumnsTableFixture d2c2 = new TwoColumnsTableFixture(connection, "TransactionDest2");
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(connection, "TransactionSourceParallelWrite");
            DbDestination<MySimpleRow> dest1 = new DbDestination<MySimpleRow>(connection, "TransactionDest1", batchSize: 2);
            DbDestination<MySimpleRow> dest2 = new DbDestination<MySimpleRow>(connection, "TransactionDest2", batchSize: 2);
            Multicast<MySimpleRow> multicast = new Multicast<MySimpleRow>();

            //Act & Assert
            Assert.ThrowsAny<Exception>(() =>
           {
               try
               {
                   connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
                   source.LinkTo(multicast);
                   multicast.LinkTo(dest1);
                   multicast.LinkTo(dest2);

                   source.Execute();
                   dest1.Wait();
                   dest2.Wait();
               }
               catch
               {
                   throw;
               }
               finally
               {
                   connection.RollbackTransaction();
                   connection.Close();
               }
           });
            if (connection.GetType() == typeof(MySqlConnectionManager))
                Task.Delay(200).Wait(); //MySql needs a little bit longer to free resources
        }

        [Fact]
        public void OneTransactionAndParallelWritingWithMARS()
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(SqlConnection, "TransactionSourceParallelWrite");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c1 = new TwoColumnsTableFixture(SqlConnection, "TransactionDest1");
            TwoColumnsTableFixture d2c2 = new TwoColumnsTableFixture(SqlConnection, "TransactionDest2");
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "TransactionSourceParallelWrite");

            string constring = $"{Config.SqlConnection.RawConnectionString("DataFlow")};MultipleActiveResultSets=True;";
            var marscon = new SqlConnectionManager(constring);
            DbDestination<MySimpleRow> dest1 = new DbDestination<MySimpleRow>(marscon, "TransactionDest1", batchSize: 2);
            DbDestination<MySimpleRow> dest2 = new DbDestination<MySimpleRow>(marscon, "TransactionDest2", batchSize: 2);
            Multicast<MySimpleRow> multicast = new Multicast<MySimpleRow>();

            //Act & Assert
            marscon.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            source.LinkTo(multicast);
            multicast.LinkTo(dest1);
            multicast.LinkTo(dest2);

            source.Execute();
            dest1.Wait();
            dest2.Wait();
            marscon.CommitTransaction();

            d2c1.AssertTestData();
            d2c1.AssertTestData();
        }

    }
}
