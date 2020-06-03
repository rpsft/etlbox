using ETLBox.Connection;
using ETLBox.DataFlow; using ETLBox.DataFlow.Connectors; using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class ReuseDataFlowAsyncTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public ReuseDataFlowAsyncTests(DataFlowDatabaseFixture dbFixture)
        {

        }

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
                var x = new MyData() { Key = key, Value = value };
                _source = new MemorySource<MyData>();
                _source.DataAsList.Add(x);
                _destination = new CustomDestination<MyData>(x => Data.Add(x.Key, x));
                _source.LinkTo(_destination);
                _source.ExecuteAsync();
            }

            public IDictionary<long, MyData> Data { get; } = new Dictionary<long, MyData>();

            public Task Initialized => _destination.Completion;
        }





    }
}
