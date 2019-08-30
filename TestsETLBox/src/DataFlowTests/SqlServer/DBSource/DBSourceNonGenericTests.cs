using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [Collection("Sql Server DataFlow")]
    public class DBSourceNonGenericTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public DBSourceNonGenericTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        [Fact]
        public void UsingTableDefinitions()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("Destination");

            //Act
            DBSource source = new DBSource()
            {
                SourceTableDefinition = source2Columns.TableDefinition,
                ConnectionManager = Connection
            };
            DBDestination dest = new DBDestination()
            {
                DestinationTableDefinition = dest2Columns.TableDefinition,
                ConnectionManager = Connection
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }


        [Fact]
        public void WithSql()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("Destination");

            //Act
            DBSource source = new DBSource()
            {
                Sql = "SELECT Col1, Col2 FROM dbo.Source",
                ConnectionManager = Connection
            };
            DBDestination dest = new DBDestination(Connection, "dbo.Destination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void WithSqlNotMatchingColumns()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();
            SqlTask.ExecuteNonQuery(Connection, "Create destination table", @"CREATE TABLE DestinationNotMatching
                (Col3 nvarchar(100) null, Col4 nvarchar(100) null, Col1 nvarchar(100) null)");

            //Act
            DBSource source = new DBSource()
            {
                Sql = "SELECT Col1, Col2 FROM dbo.Source",
                ConnectionManager = Connection
            };
            DBDestination dest = new DBDestination(Connection, "dbo.DestinationNotMatching");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "DestinationNotMatching"));
            Assert.Equal(1, RowCountTask.Count(Connection, "DestinationNotMatching", "Col3 = '1' AND Col4='Test1'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "DestinationNotMatching", "Col3 = '2' AND Col4='Test2'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "DestinationNotMatching", "Col3 = '3' AND Col4='Test3'"));
        }


        [Fact]
        public void WithLessColumnsInDestination()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();
            SqlTask.ExecuteNonQuery(Connection, "Create destination table", @"CREATE TABLE dbo.DestinationOneColumn
                (ColX nvarchar (100) not null )");

            //Act
            DBSource source = new DBSource(Connection, "Source");
            DBDestination dest = new DBDestination(Connection, "dbo.DestinationOneColumn");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "DestinationOneColumn"));
            Assert.Equal(1, RowCountTask.Count(Connection, "DestinationOneColumn", "ColX = '1'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "DestinationOneColumn", "ColX = '2'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "DestinationOneColumn", "ColX = '3'"));
        }





    }
}
