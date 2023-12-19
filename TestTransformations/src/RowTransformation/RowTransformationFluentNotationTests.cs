using System.Threading.Tasks;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.RowTransformation
{
    public class RowTransformationFluentNotationTests : TransformationsTestBase
    {
        public RowTransformationFluentNotationTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void Linking3Transformations()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "SourceMultipleLinks"
            );
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "DestinationMultipleLinks"
            );

            DbSource<string[]> source = new DbSource<string[]>(
                SqlConnection,
                "SourceMultipleLinks"
            );
            DbDestination<string[]> dest = new DbDestination<string[]>(
                SqlConnection,
                "DestinationMultipleLinks"
            );
            RowTransformation<string[]> trans1 = new RowTransformation<string[]>(row => row);
            RowTransformation<string[]> trans2 = new RowTransformation<string[]>(row => row);
            RowTransformation<string[]> trans3 = new RowTransformation<string[]>(row => row);

            //Act
            source.LinkTo(trans1).LinkTo(trans2).LinkTo(trans3).LinkTo(dest);
            Task sourceT = source.ExecuteAsync();
            Task destT = dest.Completion;

            //Assert
            sourceT.Wait();
            destT.Wait();
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void UsingFluentVoidPredicate()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "SourceMultipleLinks"
            );
            source2Columns.InsertTestData();
            source2Columns.InsertTestDataSet2();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "DestinationMultipleLinks"
            );

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "SourceMultipleLinks"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "DestinationMultipleLinks"
            );
            RowTransformation<MySimpleRow> trans1 = new RowTransformation<MySimpleRow>(row => row);

            //Act
            source.LinkTo(trans1, row => row.Col1 < 4, row => row.Col1 >= 4).LinkTo(dest);
            Task sourceT = source.ExecuteAsync();
            Task destT = dest.Completion;

            //Assert
            sourceT.Wait();
            destT.Wait();
            dest2Columns.AssertTestData();
        }

        public class MyOtherRow
        {
            [ColumnMap("Col1")]
            public int ColA { get; set; }

            [ColumnMap("Col2")]
            public string ColB { get; set; }
        }

        [Fact]
        public void UsingDifferentObjectTypes()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "SourceMultipleLinks"
            );
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "DestinationMultipleLinks"
            );

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "SourceMultipleLinks"
            );
            DbDestination<MyOtherRow> dest = new DbDestination<MyOtherRow>(
                SqlConnection,
                "DestinationMultipleLinks"
            );
            RowTransformation<MySimpleRow, MyOtherRow> trans1 = new RowTransformation<
                MySimpleRow,
                MyOtherRow
            >(row => new MyOtherRow { ColA = row.Col1, ColB = row.Col2 });

            //Act
            source.LinkTo<MyOtherRow>(trans1).LinkTo(dest);

            //Assert
            source.Execute();
            dest.Wait();
            dest2Columns.AssertTestData();
        }
    }
}
