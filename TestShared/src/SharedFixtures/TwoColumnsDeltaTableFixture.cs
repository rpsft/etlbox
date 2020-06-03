using ETLBox;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBoxTests.Helper;
using System.Collections.Generic;

namespace ETLBoxTests.Fixtures
{
    public class TwoColumnsDeltaTableFixture
    {
        public IConnectionManager Connection { get; set; } = Config.SqlConnection.ConnectionManager("DataFlow");
        public TableDefinition TableDefinition { get; set; }
        public string TableName { get; set; }
        public TwoColumnsDeltaTableFixture(string tableName)
        {
            this.TableName = tableName;
            RecreateTable();
        }

        public TwoColumnsDeltaTableFixture(IConnectionManager connection, string tableName)
        {
            this.Connection = connection;
            this.TableName = tableName;
            RecreateTable();
        }

        public void RecreateTable()
        {
            DropTableTask.DropIfExists(Connection, TableName);

            TableDefinition = new TableDefinition(TableName
                , new List<TableColumn>() {
                new TableColumn("Col1", "INT", allowNulls: false),
                new TableColumn("Col2", "NVARCHAR(100)", allowNulls: true),
                new TableColumn("ChangeDate", "DATETIME", allowNulls: false),
                new TableColumn("ChangeAction", "INT", allowNulls: false),
            });
            TableDefinition.CreateTable(Connection);
        }
    }
}
