using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowMultiplicationErrorLinkingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowMultiplicationErrorLinkingTests(DataFlowDatabaseFixture dbFixture)
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
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("RowMultiplicationSource");
            source2Columns.InsertTestData();

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "RowMultiplicationSource");
            RowMultiplication<MySimpleRow> multiplication = new RowMultiplication<MySimpleRow>(
                row =>
                {
                    List<MySimpleRow> result = new List<MySimpleRow>();
                    result.Add(row);
                    if (row.Col1 == 2) throw new Exception("Error in Flow!");
                    return result;

                });
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();


            //Act
            source.LinkTo(multiplication);
            multiplication.LinkTo(dest);
            multiplication.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Collection<ETLBoxError>(errorDest.Data,
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }

       

        [Fact]
        public void ThrowExceptionWithoutHandling()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("RowMultiplicationSource");
            source2Columns.InsertTestData();

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "RowMultiplicationSource");
            RowMultiplication<MySimpleRow> multiplication = new RowMultiplication<MySimpleRow>(
                row =>
                {
                    List<MySimpleRow> result = new List<MySimpleRow>();
                    result.Add(row);
                    if (row.Col1 == 2) throw new Exception("Error in Flow!");
                    return result;

                });
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act & Assert
            source.LinkTo(multiplication);
            multiplication.LinkTo(dest);

            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }
    }
}
