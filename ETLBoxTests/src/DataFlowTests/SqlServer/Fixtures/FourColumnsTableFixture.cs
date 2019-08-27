using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    public class FourColumnsTableFixture
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public TableDefinition TableDefinition { get; set; }
        public string TableName { get; set; }
        public FourColumnsTableFixture(string tableName)
        {
            this.TableName = tableName;
            RecreateTable(0);
        }

        public FourColumnsTableFixture(string tableName, int identityColumnIndex)
        {
            this.TableName = tableName;
            RecreateTable(identityColumnIndex);
        }

        public void RecreateTable(int identityColumnIndex)
        {
            DropTableTask.Drop(Connection, TableName);
            bool hasIdentityCol = identityColumnIndex >= 0;
            var columns = new ObservableCollection<TableColumn>()
            {
                new TableColumn("Col1", "INT", allowNulls: false, isPrimaryKey:true, isIdentity: hasIdentityCol),
                new TableColumn("Col2", "NVARCHAR(100)", allowNulls: true),
                new TableColumn("Col3", "BIGINT", allowNulls: true),
                new TableColumn("Col4", "DECIMAL(12,6)", allowNulls: false)
            };
            if (identityColumnIndex > 0)
                columns.Move(0, identityColumnIndex);
            TableDefinition = new TableDefinition(TableName, columns.ToList());
            TableDefinition.CreateTable(Connection);
        }

        public void InsertTestData()
        {
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                , $"INSERT INTO {TableName} VALUES('Test1', NULL, '1.2')");
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                , $"INSERT INTO {TableName} VALUES('Test2', 4711, '1.23')");
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                 , $"INSERT INTO {TableName} VALUES('Test3', 185, '1.234')");
        }

        public void AssertTestData()
        {
            Assert.Equal(3, RowCountTask.Count(Connection, TableName));
            Assert.Equal(1, RowCountTask.Count(Connection, TableName, "Col2 = 'Test1' AND (Col3 IS NULL OR Col3 = -1) AND Col4='1.2'"));
            Assert.Equal(1, RowCountTask.Count(Connection, TableName, "Col2 = 'Test2' AND (Col3 IS NULL OR Col3 = 4711) AND Col4='1.23'"));
            Assert.Equal(1, RowCountTask.Count(Connection, TableName, "Col2 = 'Test3' AND (Col3 IS NULL OR Col3 = 185) AND Col4='1.234'"));
        }
    }
}
