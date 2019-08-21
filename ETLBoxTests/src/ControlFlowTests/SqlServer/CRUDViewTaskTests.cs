using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.SqlServer
{
    [Collection("Sql Server ControlFlow")]
    public class CRUDViewTaskTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public CRUDViewTaskTests(DatabaseFixture dbFixture)
        { }

        [Fact]

        public void CreateView()
        {
            //Arrange
            //Act
            CRUDViewTask.CreateOrAlter(Connection,"dbo.View1", "SELECT 1 AS Test");
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects", 
                "type = 'V' AND object_id = object_id('dbo.View1')"));
      }

        [Fact]
        public void AlterView()
        {
            //Arrange
            CRUDViewTask.CreateOrAlter(Connection, "dbo.View2", "SELECT 1 AS Test");
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'V' AND object_id = object_id('dbo.View2') AND create_date = modify_date"));
            //Act
            CRUDViewTask.CreateOrAlter(Connection, "dbo.View2", "SELECT 5 AS Test");
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'V' AND object_id = object_id('dbo.View2') AND create_date <> modify_date"));
       }
    }
}
