using System.Threading.Tasks;
using ALE.ETLBox.DataFlow;

namespace TestTransformations.UseCases
{
    [Collection("DataFlow")]
    public class ReuseDataFlowAsyncTests
    {
        [Fact]
        public void RunReusableFlow()
        {
            //Arrange
            ReferenceDataFlow r1 = new ReferenceDataFlow(1, "Flow1");
            ReferenceDataFlow r2 = new ReferenceDataFlow(1, "Flow2");

            //Act
            Task.WaitAll(r1.Initialized, r2.Initialized);

            //Assert
            Assert.Equal("Flow1", r1.Data[1].Value);
            Assert.Equal("Flow2", r2.Data[1].Value);
        }

        public class MyData
        {
            public int Key { get; set; }
            public string Value { get; set; }
        }

        public class ReferenceDataFlow
        {
            private MemorySource<MyData> _source { get; set; }
            private CustomDestination<MyData> _destination { get; set; }

            public ReferenceDataFlow(int key, string value)
            {
                var x = new MyData { Key = key, Value = value };
                _source = new MemorySource<MyData>();
                _source.DataAsList.Add(x);
                _destination = new CustomDestination<MyData>(data => Data.Add(data.Key, data));
                _source.LinkTo(_destination);
                _source.ExecuteAsync();
            }

            public IDictionary<long, MyData> Data { get; } = new Dictionary<long, MyData>();

            public Task Initialized => _destination.Completion;
        }
    }
}
