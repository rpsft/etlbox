using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class CreateIndexTaskTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public CreateIndexTaskTests(DatabaseFixture dbFixture)
        { }

        [Fact]
        public void CreateIndex()
        {
            //Arrange
            string indexName = "ix_" + HashHelper.RandomString(5);
            CreateTableTask.Create(Connection, "dbo.IndexCreation1", new List<TableColumn>()
            {
                new TableColumn("Key1", "INT", allowNulls: false),
                new TableColumn("Key2", "INT", allowNulls: true),
            });
            //Act
            CreateIndexTask.Create(Connection, indexName, "dbo.IndexCreation1", 
                new List<string>() { "Key1", "Key2" });
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.indexes",
                $"name = '{indexName}'"));
        }

        [Fact]
        public void ReCreateIndex()
        {
            //Arrange
            string indexName = "ix_" + HashHelper.RandomString(5);
            CreateTableTask.Create(Connection, "dbo.IndexReCreation1", new List<TableColumn>()
            {
                new TableColumn("Key1", "INT", allowNulls: false),
                new TableColumn("Key2", "INT", allowNulls: true),
            });
            CreateIndexTask.Create(Connection, indexName, "dbo.IndexReCreation1", 
                new List<string>() { "Key1", "Key2" });
            //Act
            CreateIndexTask.Create(Connection, indexName, "dbo.IndexReCreation1", 
                new List<string>() { "Key1", "Key2" });
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.indexes",
                $"name = '{indexName}'"));
         }

        [Fact]
        public void CreateIndexWithInclude()
        {
            //Arrange
            string indexName = "ix_" + HashHelper.RandomString(5);
            CreateTableTask.Create(Connection, "dbo.IndexCreation2", new List<TableColumn>()
            {
                new TableColumn("Key1", "INT", allowNulls: false),
                new TableColumn("Key2", "CHAR(2)", allowNulls: true),
                new TableColumn("Value1", "NVARCHAR(10)", allowNulls: true),
                new TableColumn("Value2", "DECIMAL(10,2)", allowNulls: false),
            });
            //Act
            CreateIndexTask.Create(Connection, indexName, "dbo.IndexCreation2",
                new List<string>() { "Key1", "Key2" },
                new List<string>() { "Value1", "Value2" });
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.indexes",
                    $"name = '{indexName}'"));
       }
    }
}
