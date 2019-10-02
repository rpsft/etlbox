using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class CreateSchemaTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllConnectionsWithoutSQLite("ControlFlow");
        public CreateSchemaTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void CreateSchema(IConnectionManager connection)
        {
            //Arrange
            string schemaName = "s" + HashHelper.RandomString(9);
            //Act
            CreateSchemaTask.Create(connection, schemaName);
            //Assert
            Assert.Equal(1, RowCountTask.Count(connection, "sys.schemas",
                $"schema_name(schema_id) = '{schemaName}'"));

        }
    }
}
