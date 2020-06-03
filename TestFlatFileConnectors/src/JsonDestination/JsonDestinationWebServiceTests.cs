using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBox.Json;
using ETLBox.SqlServer;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Moq;
using Moq.Protected;
using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonDestinationWebServiceTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonDestinationWebServiceTests(DataFlowDatabaseFixture dbFixture)
        {
        }


        public class MySimpleRow
        {
            public string Col2 { get; set; }
            public int Col1 { get; set; }
        }

        [Fact]
        public void WriteIntoHttpClient()
        {
            //Arrange
            Mock<HttpMessageHandler> handlerMock = CreateHandlerMoq();
            HttpClient httpClient = CreateHttpClient(handlerMock);

            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList.Add(new MySimpleRow() { Col1 = 1, Col2 = "Test1" });

            //Act
            JsonDestination<MySimpleRow> dest = new JsonDestination<MySimpleRow>("http://test.test", ResourceType.Http);
            dest.HttpClient = httpClient;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            handlerMock.Protected().Verify(
               "SendAsync",
               Times.Exactly(1),
               ItExpr.Is<HttpRequestMessage>(req =>
                  req.Method == HttpMethod.Get
                  && req.RequestUri.Equals(new Uri("http://test.test"))
               ),
               ItExpr.IsAny<CancellationToken>()
            );
        }

        private Mock<HttpMessageHandler> CreateHandlerMoq()
        {
            //Arrange
            var handlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            handlerMock.Protected()
               .Setup<Task<HttpResponseMessage>>(
                  "SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>()
               )
               .ReturnsAsync(new HttpResponseMessage()
               {
                   StatusCode = HttpStatusCode.OK
               })
               .Verifiable();
            return handlerMock;
        }

        private HttpClient CreateHttpClient(Mock<HttpMessageHandler> handlerMock)
        {
            return new HttpClient(handlerMock.Object)
            {
                BaseAddress = new Uri("http://test.test/"),
            };
        }


    }
}
