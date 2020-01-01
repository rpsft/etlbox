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
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MergeJoinNonGenericTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MergeJoinNonGenericTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void MergeJoinUsingOneObject()
        {
            //Arrange
            TwoColumnsTableFixture source1Table = new TwoColumnsTableFixture("MergeJoinNonGenericSource1");
            source1Table.InsertTestData();
            TwoColumnsTableFixture source2Table = new TwoColumnsTableFixture("MergeJoinNonGenericSource2");
            source2Table.InsertTestDataSet2();
            TwoColumnsTableFixture destTable = new TwoColumnsTableFixture("MergeJoinNonGenericDestination");

            DBSource source1 = new DBSource(Connection, "MergeJoinNonGenericSource1");
            DBSource source2 = new DBSource(Connection, "MergeJoinNonGenericSource2");
            DBDestination dest = new DBDestination(Connection, "MergeJoinNonGenericDestination");

            //Act
            MergeJoin join = new MergeJoin(
                (inputRow1, inputRow2) => {
                    inputRow1[0] = (int.Parse(inputRow1[0]) + int.Parse(inputRow2[0])).ToString();
                    inputRow1[1] += inputRow2[1];
                    return inputRow1;
                });
            source1.LinkTo(join.Target1);
            source2.LinkTo(join.Target2);
            join.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "MergeJoinNonGenericDestination"));
            Assert.Equal(1, RowCountTask.Count(Connection, "MergeJoinNonGenericDestination", "Col1 = 5 AND Col2='Test1Test4'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "MergeJoinNonGenericDestination", "Col1 = 7 AND Col2='Test2Test5'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "MergeJoinNonGenericDestination", "Col1 = 9 AND Col2='Test3Test6'"));
        }

    }
}
