using System.Collections.ObjectModel;
using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;

namespace EtlBox.Database.Tests.SharedFixtures
{
    public class FourColumnsTableFixture
    {
        private readonly IConnectionManager _connection;

        public bool IsSQLiteConnection => _connection.GetType() == typeof(SQLiteConnectionManager);
        public TableDefinition TableDefinition { get; set; } = null!;
        public string TableName { get; set; }
        public ObjectNameDescriptor TN => new(TableName, _connection.QB, _connection.QE);
        public string QB => _connection.QB;
        public string QE => _connection.QE;

        public FourColumnsTableFixture(IConnectionManager connection, string tableName, int identityColumnIndex = 0)
        {
            _connection = connection;
            TableName = tableName;
            RecreateTable(identityColumnIndex);
        }

        public void RecreateTable(int identityColumnIndex)
        {
            DropTableTask.DropIfExists(_connection, TableName);
            var hasIdentityCol = identityColumnIndex >= 0;
            var columns = new ObservableCollection<TableColumn>
            {
                new(
                    "Col1",
                    "INT",
                    allowNulls: IsSQLiteConnection,
                    isPrimaryKey: true,
                    isIdentity: hasIdentityCol
                ),
                new("Col2", "NVARCHAR(100)", allowNulls: true),
                new("Col3", "BIGINT", allowNulls: true),
                new("Col4", "DECIMAL(12,6)", allowNulls: false)
            };
            if (identityColumnIndex > 0)
                columns.Move(0, identityColumnIndex);
            TableDefinition = new TableDefinition(TableName, columns.ToList());
            TableDefinition.CreateTable(_connection);
        }

        public void InsertTestData()
        {
            if (IsSQLiteConnection)
            {
                SqlTask.ExecuteNonQuery(
                    _connection,
                    "Insert demo data",
                    $"INSERT INTO {TN.QuotedFullName} (Col1, Col2, Col3, Col4) VALUES(NULL, 'Test1', NULL, '1.2')"
                );
                SqlTask.ExecuteNonQuery(
                    _connection,
                    "Insert demo data",
                    $"INSERT INTO {TN.QuotedFullName} (Col1, Col2, Col3, Col4) VALUES(NULL, 'Test2', 4711, '1.23')"
                );
                SqlTask.ExecuteNonQuery(
                    _connection,
                    "Insert demo data",
                    $"INSERT INTO {TN.QuotedFullName} (Col1, Col2, Col3, Col4) VALUES(NULL, 'Test3', 185, '1.234')"
                );
            }
            else
            {
                SqlTask.ExecuteNonQuery(
                    _connection,
                    "Insert demo data",
                    $@"INSERT INTO {TN.QuotedFullName} ({QB}Col2{QE}, {QB}Col3{QE}, {QB}Col4{QE}) VALUES('Test1', NULL, '1.2')"
                );
                SqlTask.ExecuteNonQuery(
                    _connection,
                    "Insert demo data",
                    $@"INSERT INTO {TN.QuotedFullName} ({QB}Col2{QE}, {QB}Col3{QE}, {QB}Col4{QE}) VALUES('Test2', 4711, '1.23')"
                );
                SqlTask.ExecuteNonQuery(
                    _connection,
                    "Insert demo data",
                    $@"INSERT INTO {TN.QuotedFullName} ({QB}Col2{QE},{QB}Col3{QE}, {QB}Col4{QE}) VALUES('Test3', 185, '1.234')"
                );
            }
        }

        public void AssertTestData()
        {
            Assert.Equal(3, RowCountTask.Count(_connection, TableName));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    TableName,
                    $"{QB}Col2{QE} = 'Test1' AND ({QB}Col3{QE} IS NULL OR {QB}Col3{QE} = -1) AND {QB}Col4{QE}='1.2'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    TableName,
                    $"{QB}Col2{QE} = 'Test2' AND ({QB}Col3{QE} IS NULL OR {QB}Col3{QE} = 4711) AND {QB}Col4{QE}='1.23'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    TableName,
                    $"{QB}Col2{QE} = 'Test3' AND ({QB}Col3{QE} IS NULL OR {QB}Col3{QE} = 185) AND {QB}Col4{QE}='1.234'"
                )
            );
        }
    }
}
