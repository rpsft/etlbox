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
    public class RowCountTaskTests : IClassFixture<RowCountTableFixture>
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public RowCountTaskTests(DatabaseFixture dbFixture, RowCountTableFixture rcfixture)
        { }

        [Fact]
        public void NormalCount()
        {
            //Arrange
            //Act
            int? actual = RowCountTask.Count(Connection, "RowCountTest");
            //Assert
            Assert.Equal(3, actual);
        }

        [Fact]
        public void CountWithCondition()
        {
            //Arrange
            //Act
            int? actual = RowCountTask.Count(Connection,"RowCountTest", "Col1 = 2");
            //Assert
            Assert.Equal(1, actual );
        }

        [Fact]
        public void CountWithQuickQueryMode()
        {
            //Arrange
            //Act
            int? actual = RowCountTask.Count(Connection, "RowCountTest", RowCountOptions.QuickQueryMode);
            //Assert
            Assert.Equal(3, actual);
        }


    }
}
