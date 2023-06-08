using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;

namespace TestDatabaseConnectors.DBMerge
{
    [Collection("DataFlow")]
    public class DbMergeCompositeKeysTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

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

        private void ReCreateTable(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            DropTableTask.DropIfExists(connection, TN.ObjectName);

            CreateTableTask.Create(
                connection,
                TN.ObjectName,
                new List<TableColumn>
                {
                    new("ColKey1", "INT", allowNulls: false, isPrimaryKey: true),
                    new("ColKey2", "CHAR(1)", allowNulls: false, isPrimaryKey: true),
                    new("ColValue1", "NVARCHAR(100)", allowNulls: true, isPrimaryKey: false),
                    new("ColValue2", "NVARCHAR(100)", allowNulls: true, isPrimaryKey: false),
                }
            );
        }

        private void InsertSourceData(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'I','Insert', 'Test1')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'U','Update', 'Test2')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'E','NoChange', 'Test3')"
            );
        }

        private void InsertDestinationData(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'U','Update', 'XXX')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'E','NoChange', 'Test3')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'D','Delete', 'Test4')"
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void MergeWithCompositeKey(IConnectionManager connection)
        {
            //Arrange
            ObjectNameDescriptor TNS = new ObjectNameDescriptor(
                "DBMergeSource",
                connection.QB,
                connection.QE
            );
            ObjectNameDescriptor TND = new ObjectNameDescriptor(
                "DBMergeDestination",
                connection.QB,
                connection.QE
            );
            ReCreateTable(connection, TNS);
            ReCreateTable(connection, TND);
            InsertSourceData(connection, TNS);
            InsertDestinationData(connection, TND);
            //Act
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(connection, "DBMergeSource");
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(connection, "DBMergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DBMergeDestination"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DBMergeDestination",
                    $"{TND.QB}ColKey2{TND.QE} = 'E' and {TND.QB}ColValue2{TND.QE} = 'Test3'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DBMergeDestination",
                    $"{TND.QB}ColKey2{TND.QE} = 'U' and {TND.QB}ColValue2{TND.QE} = 'Test2'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DBMergeDestination",
                    $"{TND.QB}ColKey2{TND.QE} = 'I' and {TND.QB}ColValue2{TND.QE} = 'Test1'"
                )
            );
        }
    }
}
