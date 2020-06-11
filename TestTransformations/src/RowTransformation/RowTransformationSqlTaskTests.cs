using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowTransformationSqlTaskTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowTransformationSqlTaskTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void WaitForLongRunningProcedure()
        {
            //Arrange
            CreateProcedureTask.CreateOrAlter(SqlConnection, new ProcedureDefinition()
            {
                Name = "longrunning",
                Definition = @"
WAITFOR DELAY '00:00:01';
SELECT @input + 10;",
                Parameter = new List<ProcedureParameter>()
                {
                    new ProcedureParameter() { Name ="input", DataType="INT"}
                   // new ProcedureParameter() { Name ="result", Out = true, DataType="INT"}
                }
            });
            var source = new MemorySource<MySimpleRow>();
            source.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Test1" });
            source.DataAsList.Add(new MySimpleRow() { Col1 = 2, Col2 = "Test2" });
            source.DataAsList.Add(new MySimpleRow() { Col1 = 3, Col2 = "Test3" });
            var dest = new MemoryDestination<MySimpleRow>();
            var trans = new RowTransformation<MySimpleRow>(
                row =>
                {
                    row.Col1 = SqlTask.ExecuteScalar<int>(SqlConnection, "Read from procedure",
                        $"EXEC longrunning @input = {row.Col1}") ?? 0;
                    return row;
                }
                );

            //Act
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<MySimpleRow>(dest.Data,
                row => Assert.Equal(11, row.Col1),
                row => Assert.Equal(12, row.Col1),
                row => Assert.Equal(13, row.Col1)
                );
        }
    }
}
