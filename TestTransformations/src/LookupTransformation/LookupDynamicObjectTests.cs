using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.LookupTransformation
{
    public sealed class LookupDynamicObjectTests : TransformationsTestBase, IDisposable
    {
        private readonly CultureInfo _culture;

        public LookupDynamicObjectTests(TransformationsDatabaseFixture fixture)
            : base(fixture)
        {
            _culture = CultureInfo.CurrentCulture;
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void SimpleLookupWithDynamicObject(IConnectionManager connection)
        {
            //Arrange
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                connection,
                "SourceLookupDynamicObject"
            );
            source2Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(
                connection,
                "DestinationLookupDynamicObject",
                -1
            );

            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(
                connection,
                "SourceLookupDynamicObject"
            );
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                connection,
                "DestinationLookupDynamicObject"
            );

            //Act
            List<ExpandoObject> lookupList = new List<ExpandoObject>();

            CsvSource<ExpandoObject> lookupSource = new CsvSource<ExpandoObject>(
                "res/Lookup/LookupSource.csv"
            );

            var lookup = new LookupTransformation<ExpandoObject, ExpandoObject>(
                lookupSource,
                row =>
                {
                    dynamic r = row;
                    r.Col3 = lookupList
                        .Where(lkupRow =>
                        {
                            dynamic lk = lkupRow;
                            return int.Parse(lk.Key) == r.Col1;
                        })
                        .Select(lkupRow =>
                        {
                            dynamic lk = lkupRow;
                            return lk.Column3 == string.Empty ? null : long.Parse(lk.Column3);
                        })
                        .FirstOrDefault();
                    r.Col4 = lookupList
                        .Where(lkupRow =>
                        {
                            dynamic lk = lkupRow;
                            return int.Parse(lk.Key) == r.Col1;
                        })
                        .Select(lkupRow =>
                        {
                            dynamic lk = lkupRow;
                            return double.Parse(lk.Column4);
                        })
                        .FirstOrDefault();
                    return row;
                },
                lookupList
            );

            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }

        public void Dispose()
        {
            CultureInfo.CurrentCulture = _culture;
        }
    }
}
