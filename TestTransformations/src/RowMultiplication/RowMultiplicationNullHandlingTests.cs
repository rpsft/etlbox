using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Moq;
using MySqlX.XDevAPI.Relational;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowMultiplicationNullHandlingTests
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
            RowMultiplication<MySimpleRow> multiplication = new RowMultiplication<MySimpleRow>(
                row => new List<MySimpleRow>() { row, row }
                );
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            source.LinkTo(multiplication);
            multiplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(dest.Data,
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3")
            );
        }


    }
}
