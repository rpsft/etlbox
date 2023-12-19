using ALE.ETLBox;
using ALE.ETLBox.Common;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using TestTransformations.Fixtures;

namespace TestTransformations.UseCases
{
    public class DuplicateCheckTests : TransformationsTestBase
    {
        public DuplicateCheckTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        public class Poco
        {
            public int ID { get; set; }
            public string Name { get; set; }

            [Name("Text")]
            public string Value { get; set; }
            public bool IsDuplicate { get; set; }
        }

        private static CsvSource<Poco> CreateDuplicateCsvSource(string fileName)
        {
            CsvSource<Poco> source = new CsvSource<Poco>(fileName)
            {
                Configuration =
                {
                    Delimiter = ";",
                    TrimOptions = TrimOptions.Trim,
                    MissingFieldFound = null
                }
            };
            return source;
        }

        private DbDestination<Poco> CreateDestinationTable(string tableName)
        {
            DropTableTask.DropIfExists(SqlConnection, tableName);
            var dest = new DbDestination<Poco>(SqlConnection, tableName);
            TableDefinition stagingTable = new TableDefinition(
                tableName,
                new List<TableColumn>
                {
                    new("PKey", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
                    new("ID", "INT", allowNulls: false),
                    new("Value", "NVARCHAR(100)", allowNulls: false),
                    new("Name", "NVARCHAR(100)", allowNulls: false)
                }
            );
            stagingTable.CreateTable(SqlConnection);
            return dest;
        }

        private void AssertDataWithoutDuplicates()
        {
            Assert.Equal(3, RowCountTask.Count(SqlConnection, "dbo.DuplicateCheck"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "dbo.DuplicateCheck",
                    "ID = 1 AND Name='ROOT' AND Value = 'Lorem ipsum'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "dbo.DuplicateCheck",
                    "ID = 2 AND Name='TEST 2' AND Value = 'Lalandia'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "dbo.DuplicateCheck",
                    "ID = 3 AND Name='TEST 3' AND Value = 'XX'"
                )
            );
        }

        [Fact]
        public void DuplicateCheckInRowTrans()
        {
            //Arrange
            CsvSource<Poco> source = CreateDuplicateCsvSource("res/UseCases/DuplicateCheck.csv");
            List<int> IDs = new List<int>(); //at the end of the flow, this list will contain all IDs of your source

            //Act
            RowTransformation<Poco, Poco> rowTrans = new RowTransformation<Poco, Poco>(input =>
            {
                if (IDs.Contains(input.ID))
                    input.IsDuplicate = true;
                else
                    IDs.Add(input.ID);
                return input;
            });

            Multicast<Poco> multicast = new Multicast<Poco>();
            DbDestination<Poco> dest = CreateDestinationTable("dbo.DuplicateCheck");
            VoidDestination<Poco> trash = new VoidDestination<Poco>();

            source.LinkTo(rowTrans);
            rowTrans.LinkTo(multicast);
            multicast.LinkTo(dest, input => !input.IsDuplicate);
            multicast.LinkTo(trash, input => input.IsDuplicate);

            source.Execute();
            dest.Wait();
            trash.Wait();

            //Assert
            AssertDataWithoutDuplicates();
        }

        [Fact]
        public void DuplicateCheckWithBlockTrans()
        {
            //Arrange
            CsvSource<Poco> source = CreateDuplicateCsvSource("res/UseCases/DuplicateCheck.csv");

            //Act
            BlockTransformation<Poco> blockTrans = new BlockTransformation<Poco>(inputList =>
            {
                return inputList.GroupBy(item => item.ID).Select(y => y.First()).ToList();
            });
            DbDestination<Poco> dest = CreateDestinationTable("dbo.DuplicateCheck");

            source.LinkTo(blockTrans);
            blockTrans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            AssertDataWithoutDuplicates();
        }
    }
}
