using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.RowMultiplication
{
    [Collection("DataFlow")]
    public class RowMultiplicationDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void ReturningNewDynamicObject()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "RowMultiplicationSource"
            );
            source2Columns.InsertTestData();

            DbSource source = new DbSource(SqlConnection, "RowMultiplicationSource");
            ALE.ETLBox.DataFlow.RowMultiplication multiplication =
                new ALE.ETLBox.DataFlow.RowMultiplication(row =>
                {
                    List<ExpandoObject> result = new List<ExpandoObject>();
                    dynamic r = row;
                    for (int i = 0; i <= r.Col1; i++)
                    {
                        dynamic newdynamic = new ExpandoObject();
                        newdynamic.Col3 = i * r.Col1;
                        result.Add(newdynamic);
                    }
                    return result;
                });
            MemoryDestination dest = new MemoryDestination();

            //Act
            source.LinkTo(multiplication);
            multiplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                d =>
                {
                    Assert.True(((dynamic)d).Col3 == 0);
                },
                d =>
                {
                    Assert.True(((dynamic)d).Col3 == 1);
                },
                d =>
                {
                    Assert.True(((dynamic)d).Col3 == 0);
                },
                d =>
                {
                    Assert.True(((dynamic)d).Col3 == 2);
                },
                d =>
                {
                    Assert.True(((dynamic)d).Col3 == 4);
                },
                d =>
                {
                    Assert.True(((dynamic)d).Col3 == 0);
                },
                d =>
                {
                    Assert.True(((dynamic)d).Col3 == 3);
                },
                d =>
                {
                    Assert.True(((dynamic)d).Col3 == 6);
                },
                d =>
                {
                    Assert.True(((dynamic)d).Col3 == 9);
                }
            );
        }
    }
}
