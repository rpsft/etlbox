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

            CreateTableTask.Create(connection, TN.FullName,
                new List<TableColumn>()
                {
                    new TableColumn("ColKey1", "INT", allowNulls:false, isPrimaryKey:true),
                    new TableColumn("ColKey2", "CHAR(1)", allowNulls:false, isPrimaryKey:true),
                    new TableColumn("ColValue1", "NVARCHAR(100)", allowNulls:true, isPrimaryKey:false),
                    new TableColumn("ColValue2", "NVARCHAR(100)", allowNulls:true, isPrimaryKey:false),
                });
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


        [Theory, MemberData(nameof(Connections))]
        public void MergeWithCompositeKey(IConnectionManager connection)
        {
            //Arrange
            TableNameDescriptor TNS = new TableNameDescriptor("DBMergeSource", connection);
            TableNameDescriptor TND = new TableNameDescriptor("DBMergeDestination", connection);
            ReCreateTable(connection, TNS);
            ReCreateTable(connection, TND);
            InsertSourceData(connection, TNS);
            InsertDestinationData(connection, TND);
            //Act
            DBSource<MyMergeRow> source = new DBSource<MyMergeRow>(connection, "DBMergeSource");
            DBMerge<MyMergeRow> dest = new DBMerge<MyMergeRow>(connection, "DBMergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DBMergeDestination"));
            Assert.Equal(1, RowCountTask.Count(connection, "DBMergeDestination", $"{TND.QB}ColKey2{TND.QE} = 'E' and {TND.QB}ColValue2{TND.QE} = 'Test3'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DBMergeDestination", $"{TND.QB}ColKey2{TND.QE} = 'U' and {TND.QB}ColValue2{TND.QE} = 'Test2'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DBMergeDestination", $"{TND.QB}ColKey2{TND.QE} = 'I' and {TND.QB}ColValue2{TND.QE} = 'Test1'"));
        }
    }
}
