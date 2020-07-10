using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBox.Logging;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class ErrorDestinationTests : IDisposable
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("Logging");

        public ErrorDestinationTests(LoggingDatabaseFixture dbFixture)
        {

        }

        public void Dispose()
        {
            ControlFlow.ClearSettings();
        }

        [Fact]
        public void WriteIntoMultipleDestinations()
        {
            //Arrange
            var source = new MemorySource<string[]>();
            source.DataAsList.Add(new string[] { "Test" });
            var trans = new RowTransformation<string[]>();
            trans.TransformationFunc = r => throw new Exception();
            var dest = new MemoryDestination<string[]>();

            CreateErrorTableTask.Create(SqlConnection, "error_log");
            var mc = new Multicast<ETLBoxError>();
            var errorMem = new MemoryDestination<ETLBoxError>();
            var errorDb = new DbDestination<ETLBoxError>(SqlConnection, "error_log");
            var errorCsv = new CsvDestination<ETLBoxError>("error_csv.csv");

            source.LinkTo(trans);
            trans.LinkTo(dest);

            //Act
            trans.LinkErrorTo(mc);
            mc.LinkTo(errorMem);
            mc.LinkTo(errorDb);
            mc.LinkTo(errorCsv);

            source.Execute();
            dest.Wait();
            errorMem.Wait();
            errorDb.Wait();
            errorCsv.Wait();

            //Assert
            Assert.True(errorMem.Data.Count > 0);
            Assert.True(RowCountTask.Count(SqlConnection, "error_log") > 0);
            Assert.True(File.ReadAllText("error_csv.csv").Length > 0);
        }

    }
}
