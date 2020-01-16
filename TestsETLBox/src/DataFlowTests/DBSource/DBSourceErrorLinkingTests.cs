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
            [ColumnMap("Col3")]
            public int AddCol { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void RedirectErrorWithObject(IConnectionManager connection)
        {
            //Arrange
            CreateSourceTable(connection, "DBSourceErrorLinking");
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DBDestinationErrorLinking");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(connection, "DBSourceErrorLinking");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DBDestinationErrorLinking");
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();
            source.LinkTo(dest);
            source.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            dest2Columns.AssertTestData();
            Assert.Collection<ETLBoxError>(errorDest.Data,
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                 d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithoutErrorLinking(IConnectionManager connection)
        {
            //Arrange
            CreateSourceTable(connection, "DBSourceNoErrorLinking");

            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DBDestinationNoErrorLinking");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(connection, "DBSourceNoErrorLinking");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DBDestinationNoErrorLinking");
            source.LinkTo(dest);

            //Assert
            Assert.Throws<System.FormatException>(() =>
               {
                   source.Execute();
                   dest.Wait();
               });
        }

        private static void CreateSourceTable(IConnectionManager connection, string tableName)
        {
            DropTableTask.DropIfExists(connection, tableName);

            var TableDefinition = new TableDefinition(tableName
                , new List<TableColumn>() {
                new TableColumn("Col1", "VARCHAR(100)", allowNulls: true),
                new TableColumn("Col2", "VARCHAR(100)", allowNulls: true),
                new TableColumn("Col3", "VARCHAR(100)", allowNulls: true)
            });
            TableDefinition.CreateTable(connection);
            ObjectNameDescriptor TN = new ObjectNameDescriptor(tableName, connection);
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
              , $@"INSERT INTO {TN.QuotatedFullName} VALUES('1','Test1','1')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('1.35','TestX','X')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('2','Test2', NULL)");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('X',NULL, NULL)");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('3','Test3', '3')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES('4','Test4', 'X')");
        }




    }
}
