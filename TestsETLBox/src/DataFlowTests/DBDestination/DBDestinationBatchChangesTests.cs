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
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DBDestinationBatchChangesTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBDestinationBatchChangesTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithBatchChanges(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DBDestinationBatchChanges");
            DBDestination dest = new DBDestination(connection, "DBDestinationBatchChanges", batchSize: 2)
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
            Assert.Equal(3, RowCountTask.Count(connection, "DBDestinationBatchChanges"));
            Assert.Equal(2, RowCountTask.Count(connection, "DBDestinationBatchChanges", "Col2='NewValue'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DBDestinationBatchChanges", "Col1 = 2 AND Col2='Test2'"));
        }
    }
}
