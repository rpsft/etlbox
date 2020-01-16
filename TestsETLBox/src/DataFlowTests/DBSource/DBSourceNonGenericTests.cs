using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DBSourceNonGenericTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBSourceNonGenericTests(DataFlowDatabaseFixture dbFixture)
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
            DBSource source = new DBSource()
            {
                SourceTableDefinition = source2Columns.TableDefinition,
                ConnectionManager = connection
            };
            DBDestination dest = new DBDestination()
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
            DBSource source = new DBSource()
            {
                Sql = $"SELECT {s2c.QB}Col1{s2c.QE}, {s2c.QB}Col2{s2c.QE} FROM {s2c.QB}SourceWithSql{s2c.QE}",
                ConnectionManager = connection
            };
            DBDestination dest = new DBDestination(connection, "DestinationWithSql");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
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
            DBSource source = new DBSource(connection, "source_onlynulls");
            MemoryDestination dest = new MemoryDestination();
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<string[]>(dest.Data,
                 row => Assert.True(row[0] == null && row[1] == null));
        }
    }
}
