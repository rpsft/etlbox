using ALE.ETLBox.DataFlow;
using Moq;
using Moq.Protected;
using Newtonsoft.Json;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonSourceWebServiceTests
    {
        public JsonSourceWebServiceTests()
        {
        }

        public class Todo
        {
            [JsonProperty("Id")]
            public int Key { get; set; }
            public string Title { get; set; }
        }


        [Fact]
        public void JsonFromWebService()
        {
            // Arrange
            HttpClient httpClient = MoqJsonResponse(File.ReadAllText("res/JsonSource/Todos.json"));

            //Arrange
            MemoryDestination<Todo> dest = new MemoryDestination<Todo>();

            //Act
            JsonSource<Todo> source = new JsonSource<Todo>("http://test.com/");
            source.HttpClient = httpClient;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.All(dest.Data, item => Assert.True(item.Key > 0));
            Assert.Equal(5, dest.Data.Count);
        }

        private HttpClient MoqJsonResponse(string json)
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Loose);
            handlerMock
               .Protected()
               .Setup<Task<HttpResponseMessage>>(
                  "SendAsync",
                  ItExpr.IsAny<HttpRequestMessage>(),
                  ItExpr.IsAny<CancellationToken>()
               )
               .ReturnsAsync(new HttpResponseMessage()
               {
                   StatusCode = HttpStatusCode.OK,
                   Content = new StringContent(json),
               })
               .Verifiable();

            return new HttpClient(handlerMock.Object);
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
    }
}
