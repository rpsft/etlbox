using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using Moq;
using Moq.Protected;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonSourcePaginationTests
    {
        public JsonSourcePaginationTests()
        {
        }

        public class Todo
        {
            [JsonProperty("Id")]
            public int Key { get; set; }
            public string Title { get; set; }
        }


        [Fact]
        public void PaginatedRequest()
        {
            //Arrange
            MemoryDestination<Todo> dest = new MemoryDestination<Todo>();
            int page = 1;
            //Act
            JsonSource<Todo> source = new JsonSource<Todo>();
            source.GetNextUri = c => $"res/JsonSource/Todos_Page" + page++ + ".json";
            source.HasNextUri = c => page <= 3;
            source.ResourceType = ResourceType.File;

            //source.HttpClient = httpClient;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.All(dest.Data, item => Assert.True(item.Key > 0));
            Assert.Equal(5, dest.Data.Count);
        }


        string firstRequestUnparsedToBe =
@"{
""links"" : {
  ""self"": {
    ""url"": ""http://test.com/JsonApi""
  },
  ""next"": ""http://test.com/JsonApiNext""
}
""anotherarray"" : [
  ""test"",
  {
    ""test"": ""test""
  }
]
}
";

        string secondRequestUnparsedToBe =
@"{
""after"" : 2
""unparsed"" : [
  ""test""
]
""bool"" : True
""decimal"" : 3.2
}
";
        [Fact]
        public void JsonAPIRequestWithMetaData()
        {
            //Arrange
            MemoryDestination dest = new MemoryDestination();
            bool firstRequest = true;

            //Act
            JsonSource source = new JsonSource();
            source.GetNextUri = meta =>
            {
                 return firstRequest ? $"res/JsonSource/JsonAPI.json" : $"res/JsonSource/JsonAPINext.json";
            };
            source.HasNextUri = meta =>
            {
                Assert.Equal(3, meta.ProgressCount);
                if (firstRequest)
                    Assert.Equal(firstRequestUnparsedToBe, meta.UnparsedData, ignoreCase:true, ignoreLineEndingDifferences:true, ignoreWhiteSpaceDifferences: true);
                else
                    Assert.Equal(secondRequestUnparsedToBe, meta.UnparsedData, ignoreCase: true, ignoreLineEndingDifferences: true, ignoreWhiteSpaceDifferences: true);
                bool result = firstRequest;
                firstRequest = false;
                return result;
            };
            source.ResourceType = ResourceType.File;

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, dest.Data.Count);
        }
    }
}
