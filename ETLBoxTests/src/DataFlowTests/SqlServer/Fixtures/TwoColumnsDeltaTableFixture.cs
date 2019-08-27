using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    public class TwoColumnsDeltaTableFixture
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public TableDefinition TableDefinition { get; set; }
        public string TableName { get; set; }
        public TwoColumnsDeltaTableFixture(string tableName)
        {
            this.TableName = tableName;
            RecreateTable();
        }

        public void RecreateTable()
        {
            DropTableTask.Drop(Connection, TableName);

            TableDefinition = new TableDefinition(TableName
                , new List<TableColumn>() {
                new TableColumn("Col1", "INT", allowNulls: false),
                new TableColumn("Col2", "NVARCHAR(100)", allowNulls: true),
                new TableColumn("ChangeDate", "DATETIME", allowNulls: false),
                new TableColumn("ChangeAction", "CHAR(1)", allowNulls: false),
            });
            TableDefinition.CreateTable(Connection);
        }
    }
}
