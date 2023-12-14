using System.Net.Http;
using System.Threading.Tasks;
using ALE.ETLBox.DataFlow;
using Moq;

namespace TestTransformations.Rest
{
    public class RestTransformationTests
    {
        //TODO: тест не работает, надо чинить. Не работает дессиреализация в RestMethodAsync
        [Fact]
        public void RestTransformationRestMethodAsyncTest()
        {
            //Arrange
            dynamic data = new ExpandoObject();
            data.urlRouteParameter = "Tom";
            data.urlQueryParameter = 46;
            data.port = 90210;

            MemorySource<ExpandoObject> source = new MemorySource<ExpandoObject>(new ExpandoObject[] { data });

            MemoryDestination<ExpandoObject> destination = new MemoryDestination<ExpandoObject>();

            var httpClientMock = new Mock<IHttpClient>();
            httpClientMock
                .Setup(x => x.InvokeAsync(
                    It.IsAny<string>(),
                    It.IsAny<HttpMethod>(),
                    It.IsAny<(string Key, string Value)[]>(),
                    It.IsAny<string>()))
                .ReturnsAsync(() => @"{ ""jsonResponse"" : 100}");

            RestTransformation trans1 = new RestTransformation(httpClientMock.Object);

            trans1.RestMethodInfo = new RestMethodInfo
            {
                Url = "http://test/{{urlRouteParameter}}?urlQueryParameter={{urlQueryParameter}}",
                Body = @"{""PromoActionApiInternal"": {
                        ""TeasersPath"": ""C:/Loyalty/images/teasers/"",
                        ""TeasersUrl"": ""/image/download/"",
                        ""ImageDomainUrl"": ""http://localhost:{{port}}"",
                        ""OptInCheckUrlDomainUrl"": ""http://localhost:{{port}}"",
                        ""OptInRelativeUrlTemplate"": ""/Optin/{0}/{1}/{2}""
                    }
                }",
                Headers = [ ("header1", "testHeaderValue") ],
                Method = HttpMethod.Get,
                RetryCount = 2,
                RetryInterval = 5
            };
            trans1.ResultField = "result";

            //Act
            source.LinkTo(trans1).LinkTo(destination);

            //Assert
            source.Execute();
            destination.Wait();
            
            dest2Columns.AssertTestData();

            
        }
    }
}
