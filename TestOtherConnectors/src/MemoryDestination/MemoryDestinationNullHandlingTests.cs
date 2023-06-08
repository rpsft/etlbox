using System.Collections.Generic;
using ALE.ETLBox.DataFlow;
using Xunit;

namespace TestOtherConnectors.MemoryDestination
{
    [Collection("DataFlow")]
    public class MemoryDestinationNullHandlingTests
    {
        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreWithObject()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>
            {
                DataAsList = new List<MySimpleRow>
                {
                    null,
                    new MySimpleRow { Col1 = 1, Col2 = "Test1" },
                    null,
                    new MySimpleRow { Col1 = 2, Col2 = "Test2" },
                    new MySimpleRow { Col1 = 3, Col2 = "Test3" },
                    null
                }
            };

            //Act
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3")
            );
        }
    }
}
