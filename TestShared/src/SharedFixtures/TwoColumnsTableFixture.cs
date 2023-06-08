using System.Collections.Generic;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using TestShared.Helper;
using Xunit;

namespace TestShared.SharedFixtures
{
    public class TwoColumnsTableFixture
    {
        public IConnectionManager Connection { get; set; } =
            Config.SqlConnection.ConnectionManager("DataFlow");
        public TableDefinition TableDefinition { get; set; }
        public string TableName { get; set; }

        public ObjectNameDescriptor TN => new(TableName, Connection.QB, Connection.QE);
        public string QB => Connection.QB;
        public string QE => Connection.QE;

        public TwoColumnsTableFixture(string tableName)
        {
            TableName = tableName;
            RecreateTable();
        }

        public TwoColumnsTableFixture(IConnectionManager connection, string tableName)
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
                    new("Col2", "NVARCHAR(100)", allowNulls: true)
                }
            );
            TableDefinition.CreateTable(Connection);
        }

        public void InsertTestData()
        {
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'Test1')"
            );
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(2,'Test2')"
            );
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(3,'Test3')"
            );
        }

        public void InsertTestDataSet2()
        {
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(4,'Test4')"
            );
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(5,'Test5')"
            );
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $@"INSERT INTO {TN.QuotatedFullName} VALUES(6,'Test6')"
            );
        }

        public void InsertTestDataSet3()
        {
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $"INSERT INTO {TN.QuotatedFullName} VALUES(1,'Test1')"
            );
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $"INSERT INTO {TN.QuotatedFullName} VALUES(2,NULL)"
            );
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $"INSERT INTO {TN.QuotatedFullName} VALUES(4,'TestX')"
            );
            SqlTask.ExecuteNonQuery(
                Connection,
                "Insert demo data",
                $"INSERT INTO {TN.QuotatedFullName} VALUES(10,'Test10')"
            );
        }

        public void AssertTestData()
        {
            Assert.Equal(3, RowCountTask.Count(Connection, TableName));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    Connection,
                    TableName,
                    $"{QB}Col1{QE} = 1 AND {QB}Col2{QE}='Test1'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    Connection,
                    TableName,
                    $"{QB}Col1{QE} = 2 AND {QB}Col2{QE}='Test2'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    Connection,
                    TableName,
                    $"{QB}Col1{QE} = 3 AND {QB}Col2{QE}='Test3'"
                )
            );
        }
    }
}
