using System;
using System.Net.Http;
using System.Threading;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using Moq;
using Moq.Contrib.HttpClient;
using Newtonsoft.Json;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonDestinationWebServiceTests
    {
        public JsonDestinationWebServiceTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void WriteIntoHttpClient()
        {
            //Arrange
            //Arrange
            var handler = new Mock<HttpMessageHandler>();
            string result = null;

            handler
                .SetupAnyRequest()
                .Returns(async (HttpRequestMessage request, CancellationToken _) =>
                {
                    result = await request.Content!.ReadAsStringAsync(_);
                    return new HttpResponseMessage
                    {
                        Content = new StringContent($"Hello, {result}")
                    };
                })
                .Verifiable();
            // .ReturnsResponse(HttpStatusCode.OK);

            var httpClient = handler.CreateClient();

            var source = new MemorySource<MySimpleRow>();
            var mySimpleRow = new MySimpleRow { Col1 = 1, Col2 = "Test1" };
            source.DataAsList.Add(mySimpleRow);

            //Act
            var dest = new JsonDestination<MySimpleRow>("http://test.test", ResourceType.Http)
            {
                HttpClient = httpClient
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            handler.VerifyRequest(
                message => message.Method == HttpMethod.Post &&
                           message.RequestUri == new Uri("http://test.test")
                , Times.Exactly(1)
            );
            Assert.Equal(JsonConvert.SerializeObject(new[] { mySimpleRow }, Formatting.Indented), result);
        }


        public class MySimpleRow
        {
            public string Col2 { get; set; }
            public int Col1 { get; set; }
        }
    }
}