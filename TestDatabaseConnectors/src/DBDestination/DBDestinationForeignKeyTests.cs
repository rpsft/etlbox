using ETLBox;
using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationForeignKeyTests
    {
        public static IEnumerable<object[]> Connections => Config.AllConnectionsWithoutSQLite("DataFlow");
        public DbDestinationForeignKeyTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyRow
        {
            public long Key1 { get; set; }
            public long Key2 { get; set; }
            public string Value { get; set; }
        }

        void ReCreateOtherTable(IConnectionManager connection, string tablename)
        {
            DropTableTask.DropIfExists(connection, tablename);

            CreateTableTask.Create(connection, tablename,
                new List<TableColumn>()
                {
                    new TableColumn("Id", "INT", allowNulls:false, isPrimaryKey:true),
                    new TableColumn("Other", "VARCHAR(100)", allowNulls:true, isPrimaryKey:false),
                });
            ObjectNameDescriptor TN = new ObjectNameDescriptor(tablename, connection.QB, connection.QE);
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(10,'TestX')");
        }
        void ReCreateTable(IConnectionManager connection, string tableName)
        {
            DropTableTask.DropIfExists(connection, tableName);

            CreateTableTask.Create(connection, tableName,
                new List<TableColumn>()
                {
                    new TableColumn("Key1", "INT", allowNulls:false, isPrimaryKey:true),
                    new TableColumn("Key2", "INT", allowNulls:false, isPrimaryKey:true),
                    new TableColumn("Value1", "VARCHAR(100)", allowNulls:true, isPrimaryKey:false),
                });
        }

        void AddFKConstraint(IConnectionManager connection, string sourceTableName, string referenceTableName)
        {
            ObjectNameDescriptor TN = new ObjectNameDescriptor(sourceTableName, connection.QB, connection.QE);
            ObjectNameDescriptor TNR = new ObjectNameDescriptor(referenceTableName, connection.QB, connection.QE);
            SqlTask.ExecuteNonQuery(connection, "Add FK constraint",
                $@"ALTER TABLE {TN.QuotatedFullName}
ADD CONSTRAINT constraint_fk
FOREIGN KEY ({TN.QB}Key2{TN.QE})
REFERENCES {TNR.QuotatedFullName}({TNR.QB}Id{TNR.QE})
ON DELETE CASCADE;");
        }

        void InsertTestData(IConnectionManager connection, string tableName)
        {
            ObjectNameDescriptor TN = new ObjectNameDescriptor(tableName, connection.QB, connection.QE);
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                    , $@"INSERT INTO {TN.QuotatedFullName} VALUES(1, 10 ,'Test1')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                    , $@"INSERT INTO {TN.QuotatedFullName} VALUES(2, 10 ,'Test2')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(3, 10 ,'Test3')");
        }

        [Theory, MemberData(nameof(Connections))]
        public void WriteIntoTableWithPKAndFK(IConnectionManager connection)
        {
            //Arrange
            ReCreateOtherTable(connection, "FKReferenceTable");
            ReCreateTable(connection, "FKSourceTable");
            ReCreateTable(connection, "FKDestTable");
            InsertTestData(connection, "FKSourceTable");
            AddFKConstraint(connection, "FKDestTable", "FKReferenceTable");

            DbSource<MyRow> source = new DbSource<MyRow>(connection, "FKSourceTable");

            //Act
            DbDestination<MyRow> dest = new DbDestination<MyRow>(connection, "FKDestTable");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "FKDestTable").Value);

        }
    }
}
