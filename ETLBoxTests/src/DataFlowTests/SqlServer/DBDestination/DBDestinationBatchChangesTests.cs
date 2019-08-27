using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [Collection("Sql Server DataFlow")]
    public class DBDestinationBatchChangesTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public DBDestinationBatchChangesTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        [Fact]
        public void WithBatchChanges()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DBDestinationBatchChanges");
            DBDestination dest = new DBDestination(Connection, "DBDestinationBatchChanges", batchSize: 2)
            {
                BeforeBatchWrite = rowArray =>
                                   {
                                       rowArray[0][1] = "NewValue";
                                       return rowArray;
                                   }
            };

            //Act
            CSVSource source = new CSVSource("res/CSVSource/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "DBDestinationBatchChanges"));
            Assert.Equal(2, RowCountTask.Count(Connection, "DBDestinationBatchChanges", "Col2='NewValue'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "DBDestinationBatchChanges", "Col1 = 2 AND Col2='Test2'"));
        }


    }
}
