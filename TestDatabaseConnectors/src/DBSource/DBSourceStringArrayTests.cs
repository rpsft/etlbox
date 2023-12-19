using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestDatabaseConnectors.Fixtures;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBSource
{
    public class DbSourceStringArrayTests : DatabaseConnectorsTestBase
    {
        public DbSourceStringArrayTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        [Theory, MemberData(nameof(Connections))]
        public void UsingTableDefinitions(IConnectionManager connection)
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                connection,
                "SourceTableDef"
            );
            source2Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(
                connection,
                "DestinationTableDef"
            );

            //Act
            var source = new DbSource<string[]>
            {
                SourceTableDefinition = source2Columns.TableDefinition,
                ConnectionManager = connection
            };
            var dest = new DbDestination<string[]>
            {
                DestinationTableDefinition = dest2Columns.TableDefinition,
                ConnectionManager = connection
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithSql(IConnectionManager connection)
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(connection, "SourceWithSql");
            s2C.InsertTestData();
            var d2C = new TwoColumnsTableFixture(
                connection,
                "DestinationWithSql"
            );

            //Act
            var source = new DbSource<string[]>
            {
                Sql =
                    $"SELECT {s2C.QB}Col1{s2C.QE}, {s2C.QB}Col2{s2C.QE} FROM {s2C.QB}SourceWithSql{s2C.QE}",
                ConnectionManager = connection
            };
            var dest = new DbDestination<string[]>(
                connection,
                "DestinationWithSql"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2C.AssertTestData();
        }

        [Fact]
        public void WithSelectStar()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(
                SqlConnection,
                "SourceWithSelectStar"
            );
            s2C.InsertTestData();

            //Act
            var source = new DbSource<string[]>(SqlConnection)
            {
                Sql = $"SELECT * FROM {s2C.QB}SourceWithSelectStar{s2C.QE}"
            };
            var dest = new DbDestination<string[]>(SqlConnection, "SomeTable");

            //Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }

        [Theory, MemberData(nameof(Connections))]
        public void OnlyNullValue(IConnectionManager connection)
        {
            //Arrange
            SqlTask.ExecuteNonQuery(
                connection,
                "Create destination table",
                @"CREATE TABLE source_onlynulls
                (col1 VARCHAR(100) NULL, col2 VARCHAR(100) NULL)"
            );
            SqlTask.ExecuteNonQuery(
                connection,
                "Insert demo data",
                @"INSERT INTO source_onlynulls VALUES(NULL, NULL)"
            );
            //Act
            var source = new DbSource<string[]>(connection, "source_onlynulls");
            var dest = new MemoryDestination<string[]>();
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(dest.Data, row => Assert.True(row[0] == null && row[1] == null));
        }
    }
}
