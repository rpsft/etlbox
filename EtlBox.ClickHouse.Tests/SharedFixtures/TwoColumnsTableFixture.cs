using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;

namespace EtlBox.Database.Tests.SharedFixtures
{
    public class TwoColumnsTableFixture
    {
        private readonly IConnectionManager _connection;

        public TableDefinition TableDefinition { get; set; } = null!;
        public string TableName { get; set; }

        public ObjectNameDescriptor TN => new(TableName, _connection.QB, _connection.QE);
        public string QB => _connection.QB;
        public string QE => _connection.QE;

        public TwoColumnsTableFixture(IConnectionManager connection, string tableName)
        {
            _connection = connection;
            TableName = tableName;
            RecreateTable();
        }

        public void RecreateTable()
        {
            DropTableTask.DropIfExists(_connection, TableName);

            TableDefinition = new TableDefinition(
                TableName,
                new List<TableColumn>
                {
                    new("Col1", "INT", allowNulls: false),
                    new("Col2", "NVARCHAR(100)", allowNulls: true)
                }
            );
            TableDefinition.CreateTable(_connection);
        }

        public void InsertTestData()
        {
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotedFullName} VALUES(1,'Test1')"
            );
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotedFullName} VALUES(2,'Test2')"
            );
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotedFullName} VALUES(3,'Test3')"
            );
        }

        public void InsertTestDataSet2()
        {
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotedFullName} VALUES(4,'Test4')"
            );
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotedFullName} VALUES(5,'Test5')"
            );
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotedFullName} VALUES(6,'Test6')"
            );
        }

        public void InsertTestDataSet3()
        {
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $"INSERT INTO {TN.QuotedFullName} VALUES(1,'Test1')"
            );
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $"INSERT INTO {TN.QuotedFullName} VALUES(2,NULL)"
            );
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $"INSERT INTO {TN.QuotedFullName} VALUES(4,'TestX')"
            );
            SqlTask.ExecuteNonQuery(
                _connection,
                "Insert demo data",
                $"INSERT INTO {TN.QuotedFullName} VALUES(10,'Test10')"
            );
        }

        public void AssertTestData()
        {
            Assert.Equal(3, RowCountTask.Count(_connection, TableName));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    TableName,
                    $"{QB}Col1{QE} = 1 AND {QB}Col2{QE}='Test1'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    TableName,
                    $"{QB}Col1{QE} = 2 AND {QB}Col2{QE}='Test2'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    TableName,
                    $"{QB}Col1{QE} = 3 AND {QB}Col2{QE}='Test3'"
                )
            );
        }
    }
}
