using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationMaxBufferSizeTests
    {
        public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public DbDestinationMaxBufferSizeTests(DataFlowDatabaseFixture dbFixture)
        {

        }

        [Fact]
        public void TestBoundedCapacityIsWorking()
        {
            var connection = SqlConnection;
            connection.FireTriggers = true;
            SqlTask.ExecuteNonQuery(connection, "Create test table",
                "CREATE TABLE test ( id INT NOT NULL );");
            SqlTask.ExecuteNonQuery(connection, "Add wait trigger",
                $@"CREATE TRIGGER testtrigger ON test
                    AFTER  INSERT
                    AS
                    WAITFOR DELAY '00:00:3.000';");


            var source = new MemorySource<string[]>();
            for (int i = 0; i < 20; i++)
                source.DataAsList.Add(new string[] { i.ToString() });

            var dest = new DbDestination<string[]>(connection, "test", batchSize: 2)
            {
                MaxBufferSize = 6
            };

            source.LinkTo(dest);
            var s = source.ExecuteAsync();
            var d = dest.Completion;

            while (!d.IsCompleted)
            {
                Task.Delay(500).Wait();
               //dest.Out
            }

            Assert.Equal(20, dest.ProgressCount);


        }
    }
}
