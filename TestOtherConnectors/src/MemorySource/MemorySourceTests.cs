using System.Collections.Generic;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using JetBrains.Annotations;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestOtherConnectors.MemorySource
{
    [Collection("DataFlow")]
    public class MemorySourceTests
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void DataIsFromList()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("MemoryDestination");
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "MemoryDestination"
            );

            //Act
            source.DataAsList = new List<MySimpleRow>
            {
                new() { Col1 = 1, Col2 = "Test1" },
                new() { Col1 = 2, Col2 = "Test2" },
                new() { Col1 = 3, Col2 = "Test3" }
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
