using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.Multicast
{
    public class MulticastStringArrayTests : TransformationsTestBase
    {
        public MulticastStringArrayTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SplitInto2Tables()
        {
            //Arrange
            TwoColumnsTableFixture sourceTable = new TwoColumnsTableFixture("Source");
            sourceTable.InsertTestData();
            TwoColumnsTableFixture dest1Table = new TwoColumnsTableFixture("Destination1");
            TwoColumnsTableFixture dest2Table = new TwoColumnsTableFixture("Destination2");

            DbSource<string[]> source = new DbSource<string[]>(SqlConnection, "Source");
            DbDestination<string[]> dest1 = new DbDestination<string[]>(
                SqlConnection,
                "Destination1"
            );
            DbDestination<string[]> dest2 = new DbDestination<string[]>(
                SqlConnection,
                "Destination2"
            );

            //Act
            Multicast<string[]> multicast = new Multicast<string[]>();

            source.LinkTo(multicast);
            multicast.LinkTo(dest1);
            multicast.LinkTo(dest2);
            source.Execute();
            dest1.Wait();
            dest2.Wait();

            //Assert
            dest1Table.AssertTestData();
            dest2Table.AssertTestData();
        }
    }
}
