using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.Logging;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowTransformationErrorLinkingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowTransformationErrorLinkingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void ThrowExceptionInFlow()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("RowTransExceptionTest");

            CsvSource<string[]> source = new CsvSource<string[]>("res/RowTransformation/TwoColumns.csv");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(SqlConnection, "RowTransExceptionTest");

            CreateErrorTableTask.DropAndCreate(SqlConnection, "errors");
            DbDestination<ETLBoxError> errorDest = new DbDestination<ETLBoxError>(SqlConnection, "errors");

            //Act
            RowTransformation<string[], MySimpleRow> trans = new RowTransformation<string[], MySimpleRow>(
                csvdata =>
                {
                    int no = int.Parse(csvdata[0]);
                    if (no == 2) throw new Exception("Test");
                    return new MySimpleRow()
                    {
                        Col1 = no,
                        Col2 = csvdata[1]
                    };
                });

            source.LinkTo(trans);
            trans.LinkTo(dest);
            trans.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Equal(2, RowCountTask.Count(SqlConnection, "RowTransExceptionTest"));
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "errors"));
        }

        [Fact]
        public void ThrowExceptionWithoutHandling()
        {
            //Arrange
            CsvSource<string[]> source = new CsvSource<string[]>("res/RowTransformation/TwoColumns.csv");
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            RowTransformation<string[], MySimpleRow> trans = new RowTransformation<string[], MySimpleRow>(
                csvdata => throw new InvalidOperationException("Test"));

            source.LinkTo(trans);
            trans.LinkTo(dest);

            //Assert
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });


        }
    }
}
