using System.Collections.Generic;
using System.Threading.Tasks;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using TestShared.Attributes;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests;

[Collection("ControlFlow")]
public class SqlTaskInParallelTests
{
    public SqlTaskInParallelTests(ControlFlowDatabaseFixture dbFixture)
    {
        CreateTableTask.Create(Connection, "FastParallel",
            new List<TableColumn> { new("id", "int") });
    }

    public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("ControlFlow");

    [MultiprocessorOnlyFact]
    public void FastExecutingSqlsInParallel()
    {
        //Arrange
        var array = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        //Act
        Parallel.ForEach(array, new ParallelOptions { MaxDegreeOfParallelism = 8 },
            curNr => SqlTask.ExecuteNonQuery(Connection, $"Test statement {curNr}",
                $"INSERT INTO FastParallel VALUES({curNr})")
        );
        //Assert
        Assert.Equal(10, RowCountTask.Count(Connection, "FastParallel"));
    }

    [MultiprocessorOnlyFact]
    [Trait("Category", "Performance")]
    public void LongExecutingSqlTaskInParallel()
    {
        //Arrange
        var array = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
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
        Parallel.ForEach(array, curNr =>
            Assert.Equal(5000, RowCountTask.Count(Connection, $"LongParallel{curNr}")));
    }
}