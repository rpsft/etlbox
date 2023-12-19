using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestDatabaseConnectors.Fixtures;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBSource
{
    public class DbSourceColumnMappingTests : DatabaseConnectorsTestBase
    {
        public DbSourceColumnMappingTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        public class ColumnMapRow
        {
            public long Col1 { get; set; }

            [ColumnMap("Col2")]
            public string B { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void ColumnMapping(IConnectionManager connection)
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                connection,
                "Source"
            );
            source2Columns.InsertTestData();

            //Act
            var source = new DbSource<ColumnMapRow>(connection, "Source");
            var dest = new CustomDestination<ColumnMapRow>(AssertInput);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            void AssertInput(ColumnMapRow input)
            {
                //Assert
                Assert.InRange(input.Col1, 1, 3);
                Assert.StartsWith("Test", input.B);
            }
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        public class MyExtendedRow
        {
            [ColumnMap("Col3")]
            public long? Value { get; set; }

            [ColumnMap("Col4")]
            public decimal Percentage { get; set; }

            [ColumnMap("Col1")]
            public long Id { get; set; }

            [ColumnMap("Col2")]
            public string Text { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void ColumnMappingExtended(IConnectionManager connection)
        {
            //Arrange
            var source4Columns = new FourColumnsTableFixture(
                connection,
                "SourceColumnMapping",
                identityColumnIndex: 0
            );
            source4Columns.InsertTestData();

            //Act
            var source = new DbSource<MyExtendedRow>(
                connection,
                "SourceColumnMapping"
            );
            var dest = new CustomDestination<MyExtendedRow>(input =>
            {
                //Assert
                Assert.InRange(input.Id, 1, 3);
                Assert.StartsWith("Test", input.Text);
                if (input.Id == 1)
                    Assert.Null(input.Value);
                else
                    Assert.True(input.Value > 0);
                Assert.InRange(input.Percentage, 1, 2);
            });
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }
    }
}
