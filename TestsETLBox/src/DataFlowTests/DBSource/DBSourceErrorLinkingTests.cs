using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DBSourceErrorLinkingTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBSourceErrorLinkingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void RedirectError(IConnectionManager connection)
        {
            //Arrange
            CreateSourceTable(connection);
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DBDestinationErrorLinking");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(connection, "DBSourceErrorLinking");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DBDestinationErrorLinking");
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();
            source.LinkTo(dest);
            source.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithoutErrorLinking(IConnectionManager connection)
        {
            //Arrange
            CreateSourceTable(connection);

            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DBDestinationErrorLinking");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(connection, "DBSourceErrorLinking");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DBDestinationErrorLinking");
            source.LinkTo(dest);

            //Assert
            Assert.Throws<System.ArgumentException>(() =>
               {
                   source.Execute();
                   dest.Wait();
               });
        }

        private static void CreateSourceTable(IConnectionManager connection)
        {
            DropTableTask.DropIfExists(connection, "DBSourceErrorLinking");

            var TableDefinition = new TableDefinition("DBSourceErrorLinking"
                , new List<TableColumn>() {
                new TableColumn("Col1", "VARCHAR(100)", allowNulls: true),
                new TableColumn("Col2", "VARCHAR(100)", allowNulls: true)
            });
            TableDefinition.CreateTable(connection);
            ObjectNameDescriptor TN = new ObjectNameDescriptor("DBSourceErrorLinking", connection);
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
              , $@"INSERT INTO {TN.QuotatedFullName} VALUES('1','Test1')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(NULL,'TestX')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('2','Test2')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('X',NULL)");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('3','Test3')");
        }




    }
}
