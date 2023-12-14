using System;
using System.Net.Http;
using System.Reflection.PortableExecutable;
using System.Security.Policy;
using System.Threading.Tasks;
using ALE.ETLBox.DataFlow;
using Moq;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.RestTransformation
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
            httpClientMock.Setup(x => x.InvokeAsync(It.IsAny<string>(), It.IsAny<HttpMethod>(), It.IsAny<Tuple<string, string>[]>(), It.IsAny<string>()))
                .Returns(Task.FromResult("{jsonResponse: 100}"));

            ALE.ETLBox.DataFlow.RestTransformation trans1 = new ALE.ETLBox.DataFlow.RestTransformation(httpClientMock.Object);

            trans1.RestMethodInfo = new RestMethodInfo
            {
                Url = "http://test/{{urlRouteParameter}}?urlQueryParameter={{urlQueryParameter}}",
                Body =
@"{""PromoActionApiInternal"": {
        ""TeasersPath"": ""C:/Loyalty/images/teasers/"",
        ""TeasersUrl"": ""/image/download/"",
        ""ImageDomainUrl"": ""http://localhost:{{port}}"",
        ""OptInCheckUrlDomainUrl"": ""http://localhost:{{port}}"",
        ""OptInRelativeUrlTemplate"": ""/Optin/{0}/{1}/{2}""
    }
}",
                Headers = [new Tuple<string, string>("header1", "testHeaderValue")],
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
            //dest2Columns.AssertTestData();

            //TODO: проверить результат
        }
    }
}
