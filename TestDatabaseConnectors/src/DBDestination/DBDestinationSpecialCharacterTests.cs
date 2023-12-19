using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestDatabaseConnectors.Fixtures;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBDestination
{
    public class DbDestinationSpecialCharacterTests : DatabaseConnectorsTestBase
    {
        public DbDestinationSpecialCharacterTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> SqlConnections => AllSqlConnections;

        private static void InsertTestData(IConnectionManager connection, string tableName)
        {
            var tn = new ObjectNameDescriptor(tableName, connection.QB, connection.QE);

            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(1,'\0 \"" \b \n \r \t \Z \\ \% \_ ')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(2,' '' """" ')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(3,' !""�$%&/())='' ')"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                $@"INSERT INTO {tn.QuotedFullName} VALUES(4,NULL)"
            );
        }

        [Theory, MemberData(nameof(AllOdbcConnections)), MemberData(nameof(SqlConnections))]
        public void ColumnMapping(IConnectionManager connection)
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(
                connection,
                "SpecialCharacterSource"
            );
            InsertTestData(connection, "SpecialCharacterSource");

            var d2C = new TwoColumnsTableFixture(
                connection,
                "SpecialCharacterDestination"
            );

            //Act
            var source = new DbSource<string[]>
            {
                ConnectionManager = connection,
                SourceTableDefinition = s2C.TableDefinition
            };
            var dest = new DbDestination<string[]>
            {
                ConnectionManager = connection,
                DestinationTableDefinition = d2C.TableDefinition
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(4, RowCountTask.Count(connection, "SpecialCharacterDestination"));
        }
    }
}
