using ETLBox.DataFlow; using ETLBox.DataFlow.Connectors; using ETLBox.DataFlow.Transformations;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CustomDestinationNullHandlingTests
    {
        public CustomDestinationNullHandlingTests()
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreWithObject()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = new List<MySimpleRow>()
            {
                null,
                new MySimpleRow() { Col1 = 1, Col2 = "Test1"},
                null,
                new MySimpleRow() { Col1 = 2, Col2 = "Test2"},
                new MySimpleRow() { Col1 = 3, Col2 = "Test3"},
                null
            };

            //Act
            List<MySimpleRow> result = new List<MySimpleRow>();
            CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
                row => result.Add(row)
                );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(result,
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3")
            );
        }
    }
}
