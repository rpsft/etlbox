using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using CsvHelper.Configuration.Attributes;
using Moq;
using Moq.Protected;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        }

        [Fact]
        public void PaginatedRequest()
        {
            // Arrange
            HttpClient httpClient = MoqJsonResponse(File.ReadAllText("res/JsonSource/Todos.json"));

            //Arrange
            MemoryDestination<Todo> dest = new MemoryDestination<Todo>();

            //Act
            JsonSource<Todo> source = new JsonSource<Todo>();
            source.HttpClient = httpClient;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.All(dest.Data, item => Assert.True(item.Key > 0));
        }

        private HttpClient MoqJsonResponse(string json)
        {
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Loose);
            handlerMock
               .Protected()
               // Setup the PROTECTED method to mock
               .Setup<Task<HttpResponseMessage>>(
                  "SendAsync",
                  ItExpr.IsAny<HttpRequestMessage>(),
                  ItExpr.IsAny<CancellationToken>()
               )
               // prepare the expected response of the mocked http call
               .ReturnsAsync(new HttpResponseMessage()
               {
                   StatusCode = HttpStatusCode.OK,
                   Content = new StringContent(json),
               })
               .Verifiable();

            // use real http client with mocked handler here
            var httpClient = new HttpClient(handlerMock.Object)
            {
                BaseAddress = new Uri("http://test.com/"),
            };
            return httpClient;
        }
    }
}
