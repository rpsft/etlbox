using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Definitions.DataFlow;
using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestDatabaseConnectors.src.Fixtures;

namespace TestDatabaseConnectors.src.DBMerge
{
    public class DbMergeCompositeKeysTests : DatabaseConnectorsTestBase
    {
        public DbMergeCompositeKeysTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

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

        private static void ReCreateTable(IConnectionManager connection, ObjectNameDescriptor tn)
        {
            DropTableTask.DropIfExists(connection, tn.ObjectName);

            CreateTableTask.Create(
                connection,
                tn.ObjectName,
                new List<TableColumn>
                {
                    new("ColKey1", "INT", allowNulls: false, isPrimaryKey: true),
                    new("ColKey2", "CHAR(1)", allowNulls: false, isPrimaryKey: true),
                    new("ColValue1", "NVARCHAR(100)", allowNulls: true, isPrimaryKey: false),
                    new("ColValue2", "NVARCHAR(100)", allowNulls: true, isPrimaryKey: false)
                }
            );
        }

        private static void InsertSourceData(IConnectionManager connection, ObjectNameDescriptor tn)
        {
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(1,'I','Insert', 'Test1')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(1,'U','Update', 'Test2')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(1,'E','NoChange', 'Test3')"
            );
        }

        private static void InsertDestinationData(
            IConnectionManager connection,
            ObjectNameDescriptor tn
        )
        {
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(1,'U','Update', 'XXX')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(1,'E','NoChange', 'Test3')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(1,'D','Delete', 'Test4')"
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void MergeWithCompositeKey(IConnectionManager connection)
        {
            //Arrange
            var sourceTn = new ObjectNameDescriptor(
                "DBMergeSource",
                connection.QB,
                connection.QE
            );
            var destinationTn = new ObjectNameDescriptor(
                "DBMergeDestination",
                connection.QB,
                connection.QE
            );
            ReCreateTable(connection, sourceTn);
            ReCreateTable(connection, destinationTn);
            InsertSourceData(connection, sourceTn);
            InsertDestinationData(connection, destinationTn);
            //Act
            var source = new DbSource<MyMergeRow>(connection, "DBMergeSource");
            var dest = new DbMerge<MyMergeRow>(connection, "DBMergeDestination");
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
                    $"{destinationTn.QB}ColKey2{destinationTn.QE} = 'E' and {destinationTn.QB}ColValue2{destinationTn.QE} = 'Test3'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DBMergeDestination",
                    $"{destinationTn.QB}ColKey2{destinationTn.QE} = 'U' and {destinationTn.QB}ColValue2{destinationTn.QE} = 'Test2'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DBMergeDestination",
                    $"{destinationTn.QB}ColKey2{destinationTn.QE} = 'I' and {destinationTn.QB}ColValue2{destinationTn.QE} = 'Test1'"
                )
            );
        }
    }
}
