using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.SqlServer
{
    [Collection("Sql Server ControlFlow")]
    public class SqlTaskInParallelTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public SqlTaskInParallelTests(DatabaseFixture dbFixture)
        {
            CreateTableTask.Create(Connection, "FastParallel",
                new List<TableColumn>() { new TableColumn("id", "int") });
        }

        [Fact]
        public void FastExecutingSqlsInParallel()
        {
            //Arrange
            List<int> array = new List<int>() { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            //Act
            Parallel.ForEach(array, new ParallelOptions { MaxDegreeOfParallelism = 8 },
                curNr => SqlTask.ExecuteNonQuery(Connection, $"Test statement {curNr}",
                $"INSERT INTO FastParallel VALUES({curNr})")
             );
            //Assert
            Assert.Equal(10, RowCountTask.Count(Connection, "FastParallel"));
        }

        [Fact]
        public void LongExecutingSqlTaskInParallel()
        {
            //Arrange
            List<int> array = new List<int>() { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            //Act
            Parallel.ForEach(array, new ParallelOptions { MaxDegreeOfParallelism = 8 },
                curNr => SqlTask.ExecuteNonQuery(Connection, $"Test statement {curNr}", $@"
                    DECLARE @counter INT = 0;
                    CREATE TABLE dbo.LongParallel{curNr} (
                        Col1 nvarchar(50)
                    )
                    WHILE @counter < 5000
                    BEGIN
                        SET @counter = @counter + 1;
                        INSERT INTO dbo.LongParallel{curNr} values('Lorem ipsum Lorem ipsum Lorem ipsum Lorem')
                    END
                ")
             );
            //Assert
            Parallel.ForEach(array,curNr =>
                Assert.Equal(5000, RowCountTask.Count(Connection, $"LongParallel{curNr}")));
        }
    }
}
