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
    public class ImportExportAccessTests : IDisposable
    {
        public AccessOdbcConnectionManager AccessOdbcConnection => Config.AccessOdbcConnection.ConnectionManager("DataFlow");
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public ImportExportAccessTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        private TableDefinition RecreateAccessTestTable()
        {
            try
            {
                SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Try to drop table",
                    @"DROP TABLE TestTable;");
            }
            catch { }
            TableDefinition testTable = new TableDefinition("TestTable", new List<TableColumn>() {
                new TableColumn("Field1", "NUMBER", allowNulls: true),
                new TableColumn("Field2", "CHAR", allowNulls: true)
            });
            new CreateTableTask(testTable)
            {
                ThrowErrorIfTableExists = true,
                ConnectionManager = AccessOdbcConnection
            }.Execute();
            return testTable;
        }

        //Download and configure Odbc driver for access first! This test points to access file on local path
        //Odbc driver needs to be 64bit!
        //https://www.microsoft.com/en-us/download/details.aspx?id=13255
        [Fact]
        public void CSVIntoAccess()
        {
            //Arrange
            TableDefinition testTable = RecreateAccessTestTable();

            //Act
            CSVSource source = new CSVSource("res/UseCases/AccessData.csv");
            DBDestination<string[]> dest = new DBDestination<string[]>(batchSize: 2)
            {
                DestinationTableDefinition = testTable,
                ConnectionManager = AccessOdbcConnection
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(AccessOdbcConnection, testTable.Name));
        }

        public class Data
        {
            public Double Field1 { get; set; }
            public string Field2 { get; set; }
        }

        [Fact]
        public void AccessIntoDB()
        {
            //Arrange
            TableDefinition testTable = RecreateAccessTestTable();

            SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Insert test data",
                "INSERT INTO TestTable (Field1, Field2) VALUES (1,'Test1');");
            SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Insert test data",
                "INSERT INTO TestTable (Field1, Field2) VALUES (2,'Test2');");
            SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Insert test data",
                "INSERT INTO TestTable (Field1, Field2) VALUES (3,'Test3');");

            new SqlTask("Create Target Table", $@"CREATE TABLE dbo.AccessTargetTable (
        Field1 DECIMAL not null, Field2 NVARCHAR(1000) not null)")
            {
                ConnectionManager = SqlConnection
            }.ExecuteNonQuery();

            //Act
            DBSource<Data> source = new DBSource<Data>(AccessOdbcConnection)
            {
                SourceTableDefinition = testTable
            };
            DBDestination<Data> dest = new DBDestination<Data>(SqlConnection, "dbo.AccessTargetTable");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, $"dbo.AccessTargetTable"));
        }



    }
}
