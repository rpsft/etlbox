using System;
using System.Threading.Tasks;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using Microsoft.Data.SqlClient;
using TestShared.Helper;

namespace TestDatabaseConnectors.DBSource
{
    [Collection("DataFlow")]
    public class DbSourceExceptionTests
    {
        public static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void UnknownTable()
        {
            //Arrange
            DbSource<string[]> source = new DbSource<string[]>(SqlConnection, "UnknownTable");
            MemoryDestination<string[]> dest = new MemoryDestination<string[]>();

            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }

        [Fact]
        public void UnknownTableViaTableDefinition()
        {
            //Arrange
            TableDefinition def = new TableDefinition(
                "UnknownTable",
                new List<TableColumn> { new("id", "INT") }
            );
            DbSource<string[]> source = new DbSource<string[]>
            {
                ConnectionManager = SqlConnection,
                SourceTableDefinition = def
            };
            MemoryDestination<string[]> dest = new MemoryDestination<string[]>();

            //Act & Assert
            Assert.Throws<SqlException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }

        [Fact]
        public void ErrorInSql()
        {
            //Arrange
            DbSource source = new DbSource(SqlConnection) { Sql = "SELECT XYZ FROM ABC" };
            MemoryDestination dest = new MemoryDestination();
            source.LinkTo(dest);
            //Act & Assert
            Assert.Throws<SqlException>(() =>
            {
                try
                {
                    Task s = source.ExecuteAsync();
                    Task c = dest.Completion;
                    Task.WaitAll(c, s);
                }
                catch (AggregateException e)
                {
                    throw e.InnerException!;
                }
            });
        }
    }
}
