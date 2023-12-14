using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestDatabaseConnectors.src.Fixtures;
using TestShared.src.SharedFixtures;

namespace TestDatabaseConnectors.src.DBDestination
{
    public class DbDestinationTests : DatabaseConnectorsTestBase
    {
        public DbDestinationTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public class MyExtendedRow
        {
            [ColumnMap("Col1")]
            public int Id { get; set; }

            [ColumnMap("Col3")]
            public long? Value { get; set; }

            [ColumnMap("Col4")]
            public decimal Percentage { get; set; }

            [ColumnMap("Col2")]
            public string Text { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void ColumnMapping(IConnectionManager connection)
        {
            //Arrange
            var source4Columns = new FourColumnsTableFixture(
                connection,
                "Source"
            );
            source4Columns.InsertTestData();
            var dest4Columns = new FourColumnsTableFixture(
                connection,
                "Destination",
                identityColumnIndex: 2
            );

            var source = new DbSource<string[]>(connection, "Source");
            var trans = new RowTransformation<
                string[],
                MyExtendedRow
            >(
                row =>
                    new MyExtendedRow
                    {
                        Id = int.Parse(row[0]),
                        Text = row[1],
                        Value = row[2] != null ? long.Parse(row[2]) : null,
                        Percentage = decimal.Parse(row[3])
                    }
            );

            //Act
            var dest = new DbDestination<MyExtendedRow>(
                connection,
                "Destination"
            );
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
