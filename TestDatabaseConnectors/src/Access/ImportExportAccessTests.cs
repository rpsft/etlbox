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
using TestShared.Attributes;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class ImportExportAccessTests : IDisposable
    {
        public AccessOdbcConnectionManager AccessOdbcConnection { get; set; }
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public ImportExportAccessTests(DataFlowDatabaseFixture dbFixture)
        {
            AccessOdbcConnection = Config.AccessOdbcConnection.ConnectionManager("DataFlow");
            Assert.True(AccessOdbcConnection.LeaveOpen);  //If LeaveOpen is not set to true, very strange errors may occur
        }

        public void Dispose()
        {
            AccessOdbcConnection.Close();
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
        //Odbc driver needs to be 64bit if using 64bit .NET core and 32bit if using 32bit version of .NET Core!
        //(Visual Studio 2019 16.4 changed default behaviour for xunit Tests - they now run with .NET Core 32bit versions
        //Driver Access >2016 https://www.microsoft.com/en-us/download/details.aspx?id=54920
        //Driver Access >2010 https://www.microsoft.com/en-us/download/details.aspx?id=13255
        //If LeaveOpen is not set to true, very strange errors may occur:
        //https://stackoverflow.com/questions/37432816/microsoft-ace-oledb-12-0-bug-in-multithread-scenario
        //It is recommended to leave this connection manager always open (this is why leave open is set to true by default)

        [WindowsOnlyFact]
        public void CSVIntoAccess()
        {
            //Arrange
            TableDefinition testTable = RecreateAccessTestTable();

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/Access/AccessData.csv");
            DbDestination<string[]> dest = new DbDestination<string[]>(batchSize: 2)
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
            [ColumnMap("Col1")]
            public Double Field1 { get; set; }
            public string Field2 { get; set; }
            [ColumnMap("Col2")]
            public string Field2Trimmed => Field2.Trim();
        }

        [WindowsOnlyFact]
        public void AccessIntoDBWithTableDefinition()
        {
            //Arrange
            TableDefinition testTable = RecreateAccessTestTable();
            InsertTestData();
            TwoColumnsTableFixture destTable = new TwoColumnsTableFixture(SqlConnection, "dbo.AccessTargetTableWTD");

            //Act
            DbSource<Data> source = new DbSource<Data>(AccessOdbcConnection)
            {
                SourceTableDefinition = testTable
            };
            DbDestination<Data> dest = new DbDestination<Data>(SqlConnection, "dbo.AccessTargetTableWTD");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            destTable.AssertTestData();
        }

        private void InsertTestData()
        {
            SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Insert test data",
                "INSERT INTO TestTable (Field1, Field2) VALUES (1,'Test1');");
            SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Insert test data",
                "INSERT INTO TestTable (Field1, Field2) VALUES (2,'Test2');");
            SqlTask.ExecuteNonQuery(AccessOdbcConnection, "Insert test data",
                "INSERT INTO TestTable (Field1, Field2) VALUES (3,'Test3');");
        }

        [WindowsOnlyFact]
        public void AccessIntoDB()
        {
            //Arrange
            TableDefinition testTable = RecreateAccessTestTable();
            InsertTestData();
            TwoColumnsTableFixture destTable = new TwoColumnsTableFixture(SqlConnection, "dbo.AccessTargetTable");

            //Act
            DbSource<Data> source = new DbSource<Data>(AccessOdbcConnection, "TestTable");
            DbDestination<Data> dest = new DbDestination<Data>(SqlConnection, "dbo.AccessTargetTable");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            destTable.AssertTestData();
        }


    }
}
