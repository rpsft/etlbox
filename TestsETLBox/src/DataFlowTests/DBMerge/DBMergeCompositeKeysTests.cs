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
    public class DBMergeCompositeKeysTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");


        public DBMergeCompositeKeysTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyMergeRow : MergeableRow
        {
            [IdColumn]
            public long ColKey1 { get; set; }

            [IdColumn]
            public string ColKey2 { get; set; }

            [CompareColumn]
            public string ColValue1 { get; set; }

            [CompareColumn]
            public string ColValue2 { get; set; }

        }

        void ReCreateTable(IConnectionManager connection, TableNameDescriptor TN)
        {
            DropTableTask.DropIfExists(connection, TN.FullName);

            SqlTask.ExecuteNonQuery(connection, "Insert demo data",
                $@"CREATE TABLE {TN.QuotatedFullName} (
                    ColKey1 INT NOT NULL,
                    ColKey2 CHAR(1) NOT NULL,
                    ColValue1 NVARCHAR(100) NULL,
                    ColValue2 NVARCHAR(100) NULL
                    PRIMARY KEY (ColKey1, ColKey2)
                  )");
        }


        void InsertSourceData(IConnectionManager connection, TableNameDescriptor TN)
        {
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'I','Insert', 'Test1')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'U','Update', 'Test2')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                 , $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'E','NoChange', 'Test3')");
        }

        void InsertDestinationData(IConnectionManager connection, TableNameDescriptor TN)
        {
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'U','Update', 'XXX')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                 , $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'E','NoChange', 'Test3')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'D','Delete', 'Test4')");
        }

        [Fact]
        public void MergeWithCompositeKey()
        {
            //Arrange
            TableNameDescriptor TNS = new TableNameDescriptor("DBMergeSource", SqlConnection);
            TableNameDescriptor TND = new TableNameDescriptor("DBMergeDestination", SqlConnection);
            ReCreateTable(SqlConnection, TNS);
            ReCreateTable(SqlConnection, TND);
            InsertSourceData(SqlConnection, TNS);
            InsertDestinationData(SqlConnection, TND);
            //Act
            DBSource<MyMergeRow> source = new DBSource<MyMergeRow>(SqlConnection, "DBMergeSource");
            DBMerge<MyMergeRow> dest = new DBMerge<MyMergeRow>(SqlConnection, "DBMergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, "DBMergeDestination"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "DBMergeDestination", "ColKey2 = 'E' and ColValue2 = 'Test3'"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "DBMergeDestination", "ColKey2 = 'U' and ColValue2 = 'Test2'"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "DBMergeDestination", "ColKey2 = 'I' and ColValue2 = 'Test1'"));
        }


    }
}
