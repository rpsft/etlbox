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
    public class CreateIndexTaskTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnectionManager("ControlFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public CreateIndexTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void CreateIndex(IConnectionManager connection)
        {
            //Arrange
            string indexName = "ix_IndexTest1";
            CreateTableTask.Create(connection, "IndexCreationTable1", new List<TableColumn>()
            {
                new TableColumn("Key1", "INT", allowNulls: false),
                new TableColumn("Key2", "INT", allowNulls: true),
            });

            //Act
            CreateIndexTask.CreateOrRecreate(connection, indexName, "IndexCreationTable1",
                new List<string>() { "Key1", "Key2" });

            //Assert
            Assert.True(IfIndexExistsTask.IsExisting(connection, "ix_IndexTest1", "IndexCreationTable1"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void ReCreateIndex(IConnectionManager connection)
        {
            //Arrange
            string indexName = "ix_IndexTest2";
            CreateTableTask.Create(connection, "IndexCreationTable2", new List<TableColumn>()
            {
                new TableColumn("Key1", "INT", allowNulls: false),
                new TableColumn("Key2", "INT", allowNulls: true),
            });
            CreateIndexTask.CreateOrRecreate(connection, indexName, "IndexCreationTable2",
                new List<string>() { "Key1", "Key2" });

            //Act
            CreateIndexTask.CreateOrRecreate(connection, indexName, "IndexCreationTable2",
                new List<string>() { "Key1", "Key2" });

            //Assert
            Assert.True(IfIndexExistsTask.IsExisting(connection, "ix_IndexTest2", "IndexCreationTable2"));

        }

        [Fact]
        public void CreateIndexWithInclude()
        {
            //Arrange
            string indexName = "ix_IndexTest3";
            CreateTableTask.Create(SqlConnection, "dbo.IndexCreation3", new List<TableColumn>()
            {
                new TableColumn("Key1", "INT", allowNulls: false),
                new TableColumn("Key2", "CHAR(2)", allowNulls: true),
                new TableColumn("Value1", "NVARCHAR(10)", allowNulls: true),
                new TableColumn("Value2", "DECIMAL(10,2)", allowNulls: false),
            });
            //Act
            CreateIndexTask.CreateOrRecreate(SqlConnection, indexName, "dbo.IndexCreation3",
                new List<string>() { "Key1", "Key2" },
                new List<string>() { "Value1", "Value2" });
            //Assert
            Assert.True(IfIndexExistsTask.IsExisting(SqlConnection, "ix_IndexTest3", "dbo.IndexCreation3"));
        }
    }
}
