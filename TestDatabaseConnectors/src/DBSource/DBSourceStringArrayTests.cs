using ETLBox;
using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbSourceStringArrayTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public DbSourceStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void UsingTableDefinitions(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceTableDef");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DestinationTableDef");

            //Act
            DbSource<string[]> source = new DbSource<string[]>()
            {
                SourceTableDefinition = source2Columns.TableDefinition,
                ConnectionManager = connection
            };
            DbDestination<string[]> dest = new DbDestination<string[]>()
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
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "SourceWithSql");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DestinationWithSql");

            //Act
            DbSource<string[]> source = new DbSource<string[]>()
            {
                Sql = $"SELECT {s2c.QB}Col1{s2c.QE}, {s2c.QB}Col2{s2c.QE} FROM {s2c.QB}SourceWithSql{s2c.QE}",
                ConnectionManager = connection
            };
            DbDestination<string[]> dest = new DbDestination<string[]>(connection, "DestinationWithSql");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }

        [Fact]
        public void WithSelectStar()
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(SqlConnection, "SourceWithSelectStar");
            s2c.InsertTestData();

            //Act
            DbSource<string[]> source = new DbSource<string[]>(SqlConnection)
            {
                Sql = $"SELECT * FROM {s2c.QB}SourceWithSelectStar{s2c.QE}",
            };
            DbDestination<string[]> dest = new DbDestination<string[]>(SqlConnection, "SomeTable");

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
            SqlTask.ExecuteNonQuery(connection, "Create destination table", @"CREATE TABLE source_onlynulls
                (col1 VARCHAR(100) NULL, col2 VARCHAR(100) NULL)");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO source_onlynulls VALUES(NULL, NULL)");
            //Act
            DbSource<string[]> source = new DbSource<string[]>(connection, "source_onlynulls");
            MemoryDestination<string[]> dest = new MemoryDestination<string[]>();
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<string[]>(dest.Data,
                 row => Assert.True(row[0] == null && row[1] == null));
        }
    }
}
