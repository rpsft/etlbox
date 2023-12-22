using System.Dynamic;
using System.Threading.Tasks;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using EtlBox.Logging.Database;
using TestNonParallel.Fixtures;

namespace TestNonParallel.Logging
{
    [Collection("Logging")]
    public sealed class DataFlowLoggingTests : NonParallelTestBase, IDisposable
    {
        public DataFlowLoggingTests(LoggingDatabaseFixture fixture)
            : base(fixture)
        {
            CreateLogTableTask.Create(SqlConnection);
            DatabaseLoggingConfiguration.AddDatabaseLoggingConfiguration(SqlConnection);
        }

        public void Dispose()
        {
            DropTableTask.Drop(SqlConnection, ALE.ETLBox.Common.ControlFlow.ControlFlow.LogTable);
            ALE.ETLBox.Common.ControlFlow.ControlFlow.ClearSettings();
            DataFlow.ClearSettings();
        }

        private void CreateTestTable(string tableName)
        {
            new DropTableTask(tableName)
            {
                ConnectionManager = SqlConnection,
                DisableLogging = true
            }.DropIfExists();

            new CreateTableTask(
                new TableDefinition(
                    tableName,
                    new List<TableColumn>
                    {
                        new("Col1", "INT", allowNulls: false),
                        new("Col2", "NVARCHAR(100)", allowNulls: true)
                    }
                )
            )
            {
                ConnectionManager = SqlConnection,
                DisableLogging = true
            }.Create();
        }

        private void InsertTestData(string tableName)
        {
            for (int i = 0; i < 10; i++)
                new SqlTask("Insert demo data", $"INSERT INTO {tableName} VALUES({i},'Test{i}')")
                {
                    ConnectionManager = SqlConnection,
                    DisableLogging = true
                }.ExecuteNonQuery();
        }

        [Fact]
        public void SourceAndDestinationLogging()
        {
            //Arrange
            CreateTestTable("DbSource");
            InsertTestData("DbSource");
            CreateTestTable("DbDestination");
            DbSource source = new DbSource(SqlConnection, "DbSource");
            DbDestination dest = new DbDestination(SqlConnection, "DbDestination", batchSize: 3);

            //Act
            DataFlow.LoggingThresholdRows = 3;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                4,
                new RowCountTask("etlbox_log", "task_type = 'DbSource' AND task_action = 'LOG'")
                {
                    DisableLogging = true,
                    ConnectionManager = SqlConnection
                }
                    .Count()
                    .Rows
            );
            Assert.Equal(
                4,
                new RowCountTask(
                    "etlbox_log",
                    "task_type = 'DbDestination' AND task_action = 'LOG'"
                )
                {
                    DisableLogging = true,
                    ConnectionManager = SqlConnection
                }
                    .Count()
                    .Rows
            );
        }

        [Fact]
        public void LoggingReduced()
        {
            //Arrange
            CreateTestTable("DbSource");
            InsertTestData("DbSource");
            CreateTestTable("DbDestination");
            DbSource source = new DbSource(SqlConnection, "DbSource");
            DbDestination dest = new DbDestination(SqlConnection, "DbDestination", batchSize: 3);

            //Act
            DataFlow.LoggingThresholdRows = 0;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            DataFlow.LoggingThresholdRows = 0;

            //Assert

            Assert.Equal(
                2,
                new RowCountTask("etlbox_log", "task_type = 'DbSource'")
                {
                    ConnectionManager = SqlConnection,
                    DisableLogging = true
                }
                    .Count()
                    .Rows
            );
            Assert.Equal(
                2,
                new RowCountTask("etlbox_log", "task_type = 'DbDestination'")
                {
                    ConnectionManager = SqlConnection,
                    DisableLogging = true
                }
                    .Count()
                    .Rows
            );
        }

        [Fact]
        public void LoggingInRowTransformation()
        {
            //Arrange
            CreateTestTable("DbSource");
            InsertTestData("DbSource");
            CreateTestTable("DbDestination");
            DbSource source = new DbSource(SqlConnection, "DbSource");
            DbDestination dest = new DbDestination(SqlConnection, "DbDestination", batchSize: 3);
            RowTransformation rowTrans = new RowTransformation(row => row);

            //Act
            DataFlow.LoggingThresholdRows = 3;
            source.LinkTo(rowTrans);
            rowTrans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                3,
                new RowCountTask(
                    "etlbox_log",
                    "task_type = 'RowTransformation' AND task_action = 'LOG'"
                )
                {
                    DisableLogging = true,
                    ConnectionManager = SqlConnection
                }
                    .Count()
                    .Rows
            );
        }

        [Fact]
        public void LoggingInCsvSource()
        {
            //Arrange
            CreateTestTable("DbDestination");
            CsvSource<string[]> source = new CsvSource<string[]>(
                "res/DataFlowLogging/TwoColumns.csv"
            );
            DbDestination<string[]> dest = new DbDestination<string[]>(
                SqlConnection,
                "DbDestination",
                batchSize: 3
            );

            //Act
            DataFlow.LoggingThresholdRows = 2;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                4,
                new RowCountTask("etlbox_log", "task_type LIKE 'CsvSource%' ")
                {
                    DisableLogging = true,
                    ConnectionManager = SqlConnection
                }
                    .Count()
                    .Rows
            );
        }

        [Fact]
        public void LoggingInAsyncTask()
        {
            //Arrange
            CreateTestTable("Destination4CustomSource");
            List<string> data = new List<string> { "Test1", "Test2", "Test3" };
            int readIndex = 0;

            ExpandoObject ReadData()
            {
                dynamic r = new ExpandoObject();
                r.Col1 = readIndex.ToString();
                r.Col2 = data[readIndex];
                readIndex++;
                return r;
            }

            bool EndOfData() => readIndex >= data.Count;

            //Act
            CustomSource source = new CustomSource(ReadData, EndOfData);
            DbDestination dest = new DbDestination(SqlConnection, "Destination4CustomSource");
            source.LinkTo(dest);
            Task sourceT = source.ExecuteAsync();
            Task destT = dest.Completion;

            //Assert
            sourceT.Wait();
            destT.Wait();

            //Assert
            Assert.Equal(
                3,
                new RowCountTask("etlbox_log", "task_type = 'CustomSource'")
                {
                    ConnectionManager = SqlConnection,
                    DisableLogging = true
                }
                    .Count()
                    .Rows
            );
            Assert.Equal(
                3,
                new RowCountTask("etlbox_log", "task_type = 'DbDestination'")
                {
                    ConnectionManager = SqlConnection,
                    DisableLogging = true
                }
                    .Count()
                    .Rows
            );
        }
    }
}
