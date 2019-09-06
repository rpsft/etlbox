using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    public class TwoColumnsTableFixture
    {
        public IConnectionManager Connection { get; set; } = Config.SqlConnectionManager("DataFlow");
        public TableDefinition TableDefinition { get; set; }
        public string TableName { get; set; }
        public TwoColumnsTableFixture(string tableName)
        {
            this.TableName = tableName;
            RecreateTable();
        }

        public TwoColumnsTableFixture(IConnectionManager connection, string tableName)
        {
            this.Connection = connection;
            this.TableName = tableName;
            RecreateTable();
        }

        public void RecreateTable()
        {
            DropTableTask.Drop(Connection, TableName);

            TableDefinition = new TableDefinition(TableName
                , new List<TableColumn>() {
                new TableColumn("Col1", "INT", allowNulls: false),
                new TableColumn("Col2", "NVARCHAR(100)", allowNulls: true)
            });
            TableDefinition.CreateTable(Connection);
        }

        public void InsertTestData()
        {
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                , $"INSERT INTO {TableName} VALUES(1,'Test1')");
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                , $"INSERT INTO {TableName} VALUES(2,'Test2')");
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                 , $"INSERT INTO {TableName} VALUES(3,'Test3')");
        }

        public void InsertTestDataSet2()
        {
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                , $"INSERT INTO {TableName} VALUES(4,'Test4')");
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                , $"INSERT INTO {TableName} VALUES(5,'Test5')");
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                 , $"INSERT INTO {TableName} VALUES(6,'Test6')");
        }

        public void InsertTestDataSet3()
        {
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                , $"INSERT INTO {TableName} VALUES(1,'Test1')");
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                , $"INSERT INTO {TableName} VALUES(2,NULL)");
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                , $"INSERT INTO {TableName} VALUES(4,'TestX')");
            SqlTask.ExecuteNonQuery(Connection, "Insert demo data"
                 , $"INSERT INTO {TableName} VALUES(10,'Test10')");
        }

        public void AssertTestData()
        {
            Assert.Equal(3, RowCountTask.Count(Connection, TableName));
            Assert.Equal(1, RowCountTask.Count(Connection, TableName, "Col1 = 1 AND Col2='Test1'"));
            Assert.Equal(1, RowCountTask.Count(Connection, TableName, "Col1 = 2 AND Col2='Test2'"));
            Assert.Equal(1, RowCountTask.Count(Connection, TableName, "Col1 = 3 AND Col2='Test3'"));
        }
    }
}
