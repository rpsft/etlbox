using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;

namespace EtlBox.Database.Tests.SharedFixtures
{
    public class TwoColumnsDeltaTableFixture
    {
        private readonly IConnectionManager _connection;

        public TableDefinition TableDefinition { get; set; } = null!;

        public string TableName { get; set; }

        public TwoColumnsDeltaTableFixture(IConnectionManager connection, string tableName)
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
                    new("Col2", "NVARCHAR(100)", allowNulls: true),
                    new("ChangeDate", "DATETIME", allowNulls: false),
                    new("ChangeAction", "INT", allowNulls: false)
                }
            );
            TableDefinition.CreateTable(_connection);
        }
    }
}
