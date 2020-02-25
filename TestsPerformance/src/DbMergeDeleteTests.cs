using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace ALE.ETLBoxTests.Performance
{
    [Collection("Performance")]
    public class DbMergeDeleteTests
    {
        private readonly ITestOutputHelper output;

        SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("Performance");

        public DbMergeDeleteTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        public class MyMergeRow : MergeableRow
        {
            [IdColumn]
            public Guid Id { get; set; }

            [CompareColumn]
            public DateTimeOffset LastUpdated { get; set; }

            public string Value { get; set; }
        }

        [Theory,
            InlineData(10000, 5000)]
        public void CSVIntoMemDest(int rowsInDest, int rowsInSource)
        {
            //Arrange
            List<MyMergeRow> knownGuids = new List<MyMergeRow>();
            for (int i = 0; i < rowsInSource; i++)
                knownGuids.Add(new MyMergeRow() { 
                    Id = Guid.NewGuid(), 
                    LastUpdated = DateTime.Now,
                    Value = HashHelper.RandomString(1)
                });
            MemorySource<MyMergeRow> source = new MemorySource<MyMergeRow>();
            source.Data = knownGuids;

            CreateTableTask.Create(SqlConnection, "MergeDestination",
                new List<TableColumn>()
                {
                    new TableColumn("Id", "UNIQUEIDENTIFIER", allowNulls: false, isPrimaryKey: true),
                    new TableColumn("LastUpdated","DATETIMEOFFSET", allowNulls: false),
                    new TableColumn("Value","CHAR(1)", allowNulls: false),
                });

            DbDestination<MyMergeRow> dest = new DbDestination<MyMergeRow>(SqlConnection, "MergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            MemorySource<MyMergeRow> source2 = new MemorySource<MyMergeRow>();
            source2.Data = knownGuids;
            for (int i = 0; i < rowsInDest; i++)
                knownGuids.Add(new MyMergeRow()
                {
                    Id = Guid.NewGuid(),
                    LastUpdated = DateTime.Now,
                    Value = HashHelper.RandomString(1)
                });

            DbMerge<MyMergeRow> mergeDest = new DbMerge<MyMergeRow>(SqlConnection, "MergeDestination");
            source2.LinkTo(mergeDest);
            source2.Execute();
            mergeDest.Wait();

            Assert.True(RowCountTask.Count(SqlConnection, "MergeDestination") == rowsInDest + rowsInSource);
        }

       
    }
}
