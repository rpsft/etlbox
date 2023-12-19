using System.Collections.Generic;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using TestShared.Helper;

namespace TestShared.SharedFixtures
{
    public class TwoColumnsDeltaTableFixture
    {
        public IConnectionManager Connection { get; set; } =
            Config.SqlConnection.ConnectionManager("DataFlow");
        public TableDefinition TableDefinition { get; set; }
        public string TableName { get; set; }

        public TwoColumnsDeltaTableFixture(string tableName)
        {
            TableName = tableName;
            RecreateTable();
        }

        public TwoColumnsDeltaTableFixture(IConnectionManager connection, string tableName)
        {
            Connection = connection;
            TableName = tableName;
            RecreateTable();
        }

        public void RecreateTable()
        {
            DropTableTask.DropIfExists(Connection, TableName);

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
            TableDefinition.CreateTable(Connection);
        }
    }
}
