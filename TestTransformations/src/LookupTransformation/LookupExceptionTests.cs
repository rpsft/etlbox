using ALE.ETLBox;
using ALE.ETLBox.DataFlow;

namespace TestTransformations.LookupTransformation
{
    [Collection("DataFlow")]
    public class LookupExceptionTests
    {
        public class MyDataRow
        {
            [MatchColumn("Key")]
            public long Col1 { get; set; }

            [RetrieveColumn("LookupValue")]
            public string Col2 { get; set; }
        }

        public class MyLookupRow
        {
            public long Key { get; set; }
            public long? LookupValue { get; set; }
        }

        [Fact]
        public void NoLookupSource()
        {
            //Arrange
            MemorySource<MyDataRow> source = new MemorySource<MyDataRow>();
            source.DataAsList.Add(new MyDataRow { Col1 = 1, Col2 = "Test1" });

            //Act
            var lookup = new LookupTransformation<MyDataRow, MyLookupRow>();
            MemoryDestination<MyDataRow> dest = new MemoryDestination<MyDataRow>();

            //Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                try
                {
                    source.LinkTo(lookup);
                    lookup.LinkTo(dest);
                    source.Execute();
                    dest.Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException;
                }
            });
            //Assert
        }
    }
}
