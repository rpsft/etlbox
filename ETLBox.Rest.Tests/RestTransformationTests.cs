using System.Dynamic;
using System.Globalization;
using System.Text;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Serialization;
using ALE.ETLBox.Serialization.DataFlow;
using CsvHelper.Configuration;
using ETLBox.Primitives;
using ETLBox.Rest.Models;
using FluentAssertions;
using Moq;

namespace ETLBox.Rest.Tests
{
    public class RestTransformationTests
    {
        [Fact]
        public void RestTransformation_RestMethodAsyncTest()
        {
            //Arrange
            dynamic data = new ExpandoObject();
            data.urlRouteParameter = "Tom";
            data.urlQueryParameter = 46;
            data.port = 90210;

            MemorySource<ExpandoObject> source = new MemorySource<ExpandoObject>(
                new ExpandoObject[] { data }
            );

            MemoryDestination<ExpandoObject> destination = new MemoryDestination<ExpandoObject>();

            var httpClientMock = new Mock<IHttpClient>();
            httpClientMock
                .Setup(
                    x =>
                        x.InvokeAsync(
                            It.IsAny<string>(),
                            It.IsAny<HttpMethod>(),
                            It.IsAny<Dictionary<string, string>>(),
                            It.IsAny<string>()
                        )
                )
                .ReturnsAsync(() => @"{ ""jsonResponse"" : 100}");

            RestTransformation trans1 = new RestTransformation(() => httpClientMock.Object)
            {
                RestMethodInfo = new RestMethodInfo
                {
                    Url =
                        "http://test/{{urlRouteParameter}}?urlQueryParameter={{urlQueryParameter}}",
                    Body =
                        @"{""PromoActionApiInternal"": {
                        ""TeasersPath"": ""C:/Loyalty/images/teasers/"",
                        ""TeasersUrl"": ""/image/download/"",
                        ""ImageDomainUrl"": ""http://localhost:{{port}}"",
                        ""OptInCheckUrlDomainUrl"": ""http://localhost:{{port}}"",
                        ""OptInRelativeUrlTemplate"": ""/Optin/{0}/{1}/{2}""
                    }
                }",
                    Headers = new() { { "header1", "testHeaderValue" } },
                    Method = "GET",
                    RetryCount = 2,
                    RetryInterval = 5
                },
                ResultField = "result"
            };

            //Act
            source.LinkTo(trans1).LinkTo(destination);

            //Assert
            source.Execute();
            destination.Wait();

            var dest = destination.Data?.FirstOrDefault() as IDictionary<string, object>;

            dest.Should().NotBeNull();
            var res = dest!["result"] as IDictionary<string, object>;

            res.Should().NotBeNull();
            res!["jsonResponse"].Should().Be(100);
        }

        [Fact]
        public void DataFlow_WithRestTransformation_ShouldBePassed()
        {
            var csv = GetCsv();
            var csvUri = CreateFile(csv, "csv");

            var source = new CsvSource()
            {
                Uri = csvUri,
                Configuration = new CsvConfiguration(CultureInfo.CurrentCulture)
                {
                    Delimiter = ";",
                    Escape = '#',
                    Quote = '$'
                }
            };
            var transformation = new TestRestTransformation()
            {
                RestMethodInfo = new RestMethodInfo
                {
                    Url =
                        "http://test/{{urlRouteParameter}}?urlQueryParameter={{urlQueryParameter}}",
                    Body =
                        @"{ ""PromoActionApiInternal"":
                            {
                                ""TeasersPath"": ""C:/Loyalty/images/teasers/"",
                                ""TeasersUrl"": ""/image/download/"",
                                ""ImageDomainUrl"": ""http://localhost:{{port}}"",
                                ""OptInCheckUrlDomainUrl"": ""http://localhost:{{port}}"",
                                ""OptInRelativeUrlTemplate"": ""/Optin/{0}/{1}/{2}""
                            }
                        }",
                    Headers = new Dictionary<string, string>()
                    {
                        ["header1"] = "testHeaderValue",
                        ["header2"] = "testHeaderValue2"
                    },
                    Method = "GET",
                    RetryCount = 2,
                    RetryInterval = 5
                },
                ResultField = "result",
            };

            var destination = new MemoryDestination<ExpandoObject>();

            source.LinkTo(transformation);
            transformation.LinkTo(destination);
            source.Execute();
            destination.Wait();

            var resultingData = destination.Data?.FirstOrDefault() as IDictionary<string, object>;

            resultingData.Should().NotBeNull();
            var res = resultingData!["result"] as IDictionary<string, object>;

            res.Should().NotBeNull();
            res!["jsonResponse"].Should().Be(100);
        }

        [Fact]
        public void RestTransformation_ShouldReturnErrorField()
        {
            //Arrange
            dynamic data = new ExpandoObject();
            data.urlRouteParameter = "Tom";
            data.urlQueryParameter = 46;
            data.port = 90210;

            MemorySource<ExpandoObject> source = new MemorySource<ExpandoObject>(
                new ExpandoObject[] { data }
            );

            MemoryDestination<ExpandoObject> destination = new MemoryDestination<ExpandoObject>();

            var httpClientMock = new Mock<IHttpClient>();
            httpClientMock
                .Setup(
                    x =>
                        x.InvokeAsync(
                            It.IsAny<string>(),
                            It.IsAny<HttpMethod>(),
                            It.IsAny<Dictionary<string, string>>(),
                            It.IsAny<string>()
                        )
                )
                .Throws(() => new InvalidOperationException("Some error occured"));

            RestTransformation trans1 = new RestTransformation(() => httpClientMock.Object)
            {
                RestMethodInfo = new RestMethodInfo
                {
                    Url =
                        "http://test/{{urlRouteParameter}}?urlQueryParameter={{urlQueryParameter}}",
                    Body =
                        @"{""PromoActionApiInternal"": {
                        ""TeasersPath"": ""C:/Loyalty/images/teasers/"",
                        ""TeasersUrl"": ""/image/download/"",
                        ""ImageDomainUrl"": ""http://localhost:{{port}}"",
                        ""OptInCheckUrlDomainUrl"": ""http://localhost:{{port}}"",
                        ""OptInRelativeUrlTemplate"": ""/Optin/{0}/{1}/{2}""
                    }
                }",
                    Headers = new() { { "header1", "testHeaderValue" } },
                    Method = "GET",
                    RetryCount = 2,
                    RetryInterval = 5
                },
                ResultField = "result",
                ExceptionField = "error",
                FailOnError = false
            };

            //Act
            source.LinkTo(trans1).LinkTo(destination);

            //Assert
            source.Execute();
            destination.Wait();

            var dest = destination.Data?.FirstOrDefault() as IDictionary<string, object>;

            dest.Should().NotBeNull();
            var res = dest!["result"] as IDictionary<string, object>;

            res.Should().NotBeNull();

            res!["error"].Should().Be("Exception: Some error occured");
        }

        [Fact]
        public void RestTransformation_ShouldReturnErrorFieldFromResponse()
        {
            //Arrange
            dynamic data = new ExpandoObject();
            data.urlRouteParameter = "Tom";
            data.urlQueryParameter = 46;
            data.port = 90210;

            MemorySource<ExpandoObject> source = new MemorySource<ExpandoObject>(
                new ExpandoObject[] { data }
            );

            MemoryDestination<ExpandoObject> destination = new MemoryDestination<ExpandoObject>();

            var httpClientMock = new Mock<IHttpClient>();
            httpClientMock
                .Setup(
                    x =>
                        x.InvokeAsync(
                            It.IsAny<string>(),
                            It.IsAny<HttpMethod>(),
                            It.IsAny<Dictionary<string, string>>(),
                            It.IsAny<string>()
                        )
                )
                .ReturnsAsync(() => @"{ ""errorCode"": 404, ""errorMessage"": ""NotFound"" }");

            RestTransformation trans1 = new RestTransformation(() => httpClientMock.Object)
            {
                RestMethodInfo = new RestMethodInfo
                {
                    Url =
                        "http://test/{{urlRouteParameter}}?urlQueryParameter={{urlQueryParameter}}",
                    Body =
                        @"{""PromoActionApiInternal"": {
                        ""TeasersPath"": ""C:/Loyalty/images/teasers/"",
                        ""TeasersUrl"": ""/image/download/"",
                        ""ImageDomainUrl"": ""http://localhost:{{port}}"",
                        ""OptInCheckUrlDomainUrl"": ""http://localhost:{{port}}"",
                        ""OptInRelativeUrlTemplate"": ""/Optin/{0}/{1}/{2}""
                    }
                }",
                    Headers = new() { { "header1", "testHeaderValue" } },
                    Method = "GET",
                    RetryCount = 2,
                    RetryInterval = 5
                },
                ResultField = "result",
                ExceptionField = "errorMessage",
                FailOnError = false
            };

            //Act
            source.LinkTo(trans1).LinkTo(destination);

            //Assert
            source.Execute();
            destination.Wait();

            var dest = destination.Data?.FirstOrDefault() as IDictionary<string, object>;

            dest.Should().NotBeNull();
            var res = dest!["result"] as IDictionary<string, object>;

            res.Should().NotBeNull();

            res!["errorMessage"].Should().Be("NotFound");
        }

        private static string GetCsv()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("urlRouteParameter;urlQueryParameter;port");
            stringBuilder.AppendLine("Tom;46;90210");
            return stringBuilder.ToString();
        }

        private static string CreateFile(string content, string ext)
        {
            var path = Path.Combine(Path.GetTempPath(), $"{Path.GetRandomFileName()}.{ext}");
            File.WriteAllText(path, content);
            return path;
        }

        [Serializable]
        public class EtlDataFlowStep : IDataFlow, IXmlSerializable
        {
            public Guid? ReferenceId { get; set; }

            public string Name { get; set; }

            public int? TimeoutMilliseconds { get; set; }

            public IDataFlowSource<ExpandoObject> Source { get; set; }

            public IList<IDataFlowDestination<ExpandoObject>> Destinations { get; set; }
            public IList<IDataFlowDestination<ETLBoxError>> ErrorDestinations { get; set; }

            public XmlSchema GetSchema() => null;

            public virtual void ReadXml(XmlReader reader)
            {
                var xmlReader = new DataFlowXmlReader(this);
                xmlReader.Read(reader);
            }

            public void WriteXml(XmlWriter writer)
            {
                throw new NotSupportedException();
            }

            public void Invoke()
            {
                Source.Execute(CancellationToken.None);
                var tasks = Destinations.Select(d => d.Completion).ToArray<Task>();
                Task.WaitAll(tasks);
            }
        }

        public class TestRestTransformation : RestTransformation
        {
            public TestRestTransformation()
                : base(() => CreateClient()) { }

            private static IHttpClient CreateClient()
            {
                var httpClientMock = new Mock<IHttpClient>();
                httpClientMock
                    .Setup(
                        x =>
                            x.InvokeAsync(
                                It.IsAny<string>(),
                                It.IsAny<HttpMethod>(),
                                It.IsAny<Dictionary<string, string>>(),
                                It.IsAny<string>()
                            )
                    )
                    .ReturnsAsync(() => @"{ ""jsonResponse"" : 100}");

                return httpClientMock.Object;
            }
        }
    }
}
