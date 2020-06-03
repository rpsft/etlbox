using ETLBox;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationSpecialCharacterTests
    {
        public static IEnumerable<object[]> OdbcConnections => Config.AllOdbcConnections("DataFlow");
        public static IEnumerable<object[]> SqlConnections => Config.AllSqlConnections("DataFlow");

        public DbDestinationSpecialCharacterTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        private void InsertTestData(IConnectionManager connection, string tableName)
        {
            var TN = new ObjectNameDescriptor(tableName, connection.QB, connection.QE);

            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(1,'\0 \"" \b \n \r \t \Z \\ \% \_ ')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(2,' '' """" ')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                 , $@"INSERT INTO {TN.QuotatedFullName} VALUES(3,' !""�$%&/())='' ')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {TN.QuotatedFullName} VALUES(4,NULL)");
        }

        [Theory, MemberData(nameof(OdbcConnections)),
            MemberData(nameof(SqlConnections))]
        public void ColumnMapping(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "SpecialCharacterSource");
            InsertTestData(connection, "SpecialCharacterSource");

            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "SpecialCharacterDestination");

            //Act
            DbSource<string[]> source = new DbSource<string[]>()
            {
                ConnectionManager = connection,
                SourceTableDefinition = s2c.TableDefinition
            };
            DbDestination<string[]> dest = new DbDestination<string[]>()
            {
                ConnectionManager = connection,
                DestinationTableDefinition = d2c.TableDefinition
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(4, RowCountTask.Count(connection, "SpecialCharacterDestination"));
        }
    }
}
