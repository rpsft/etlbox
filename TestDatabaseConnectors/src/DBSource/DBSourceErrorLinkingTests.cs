using System.Threading.Tasks;
using ALE.ETLBox;
using ALE.ETLBox.Common;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ETLBox.Primitives;

namespace TestDatabaseConnectors.DBSource
{
    public class DbSourceErrorLinkingTests : DatabaseConnectorsTestBase
    {
        public DbSourceErrorLinkingTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }

            [ColumnMap("Col3")]
            public int AddCol { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void RedirectErrorWithObject(IConnectionManager connection)
        {
            if (connection.GetType() == typeof(SQLiteConnectionManager))
                Task.Delay(100).Wait(); //Database was locked and needs to recover after exception

            //Arrange
            CreateSourceTable(connection, "DbSourceErrorLinking");
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                connection,
                "DbDestinationErrorLinking"
            );

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                connection,
                "DbSourceErrorLinking"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                connection,
                "DbDestinationErrorLinking"
            );
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();
            source.LinkTo(dest);
            source.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            dest2Columns.AssertTestData();
            Assert.Collection(
                errorDest.Data,
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    ),
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    ),
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    )
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithoutErrorLinking(IConnectionManager connection)
        {
            //Arrange
            CreateSourceTable(connection, "DbSourceNoErrorLinking");

            var _ = new TwoColumnsTableFixture(connection, "DbDestinationNoErrorLinking");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                connection,
                "DbSourceNoErrorLinking"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                connection,
                "DbDestinationNoErrorLinking"
            );
            source.LinkTo(dest);

            //Assert
            Assert.Throws<FormatException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }

        private static void CreateSourceTable(IConnectionManager connection, string tableName)
        {
            DropTableTask.DropIfExists(connection, tableName);

            var tableDefinition = new TableDefinition(
                tableName,
                new List<TableColumn>
                {
                    new("Col1", "VARCHAR(100)", allowNulls: true),
                    new("Col2", "VARCHAR(100)", allowNulls: true),
                    new("Col3", "VARCHAR(100)", allowNulls: true)
                }
            );
            tableDefinition.CreateTable(connection);
            ObjectNameDescriptor tn = new ObjectNameDescriptor(
                tableName,
                connection.QB,
                connection.QE
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES('1','Test1','1')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES('1.35','TestX','X')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES('2','Test2', NULL)"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES('X',NULL, NULL)"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES('3','Test3', '3')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES('4','Test4', 'X')"
            );
        }
    }
}
