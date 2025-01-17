#nullable enable
using System.Collections.Concurrent;
using System.Data.Common;
using System.Dynamic;
using System.Globalization;
using System.Net;
using System.Text;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;
using ALE.ETLBox;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Serialization;
using ALE.ETLBox.Serialization.DataFlow;
using CsvHelper.Configuration;
using ETLBox.Primitives;
using ETLBox.Rest.Models;
using FluentAssertions;
using JetBrains.Annotations;
using Moq;

namespace ETLBox.Rest.Tests
{
    public class RestTransformationTests
    {
        private readonly dynamic _fakeSourceData = new ExpandoObject();

        public RestTransformationTests()
        {
            _fakeSourceData.urlRouteParameter = "Tom";
            _fakeSourceData.urlQueryParameter = 46;
            _fakeSourceData.port = 90210;
        }

        [Fact]
        public void RestTransformation_WithValidData_ShouldReturnExpectedJsonResponse()
        {
            //Arrange
            var source = new MemorySource<ExpandoObject>(new ExpandoObject[] { _fakeSourceData });
            var destination = new MemoryDestination<ExpandoObject>();
            var httpClientMock = CreateMockHttpClient(@"{ ""jsonResponse"" : 100 }");
            var trans1 = SetupRestTransformation(httpClientMock.Object);
            source.LinkTo(trans1).LinkTo(destination);

            //Act
            source.Execute();
            destination.Wait();

            //Assert
            var dest = destination.Data?.FirstOrDefault() as IDictionary<string, object?>;
            dest.Should().NotBeNull();
            dest.Should().BeOfType<ExpandoObject>();
            var hasException = dest!.TryGetValue("exception", out var exception);
            exception.Should().BeNull();
            hasException.Should().BeFalse();
            dest.Should()
                .BeEquivalentTo(
                    new Dictionary<string, object?>
                    {
                        ["urlRouteParameter"] = "Tom",
                        ["urlQueryParameter"] = 46,
                        ["port"] = 90210,
                        ["http_code"] = HttpStatusCode.OK,
                        ["result"] = new Dictionary<string, object?> { ["jsonResponse"] = 100 },
                        ["raw_response"] = "{ \"jsonResponse\" : 100 }",
                    }
                );
        }

        [Fact]
        public void DataFlow_WithRestTransformation_ShouldBePassed()
        {
            // Arrange
            var csv = GetCsv();
            var csvUri = CreateFile(csv, "csv");
            var source = new CsvSource
            {
                Uri = csvUri,
                Configuration = new CsvConfiguration(CultureInfo.CurrentCulture)
                {
                    Delimiter = ";",
                    Escape = '#',
                    Quote = '$',
                },
            };
            const string jsonString =
                "{\"name\": \"test\", \"true\": true, \"false\": false, \"null\": null, \"array\": [1,2,3], \"object\": {\"key\": \"value\"}}";
            var transformation = SetupRestTransformation(CreateMockHttpClient(jsonString).Object);
            var destination = new MemoryDestination<ExpandoObject>();

            source.LinkTo(transformation);
            transformation.LinkTo(destination);

            // Act
            source.Execute();
            destination.Wait();

            // Assert
            var resultingData = destination.Data?.FirstOrDefault() as IDictionary<string, object?>;
            resultingData.Should().NotBeNull();
            var dest = resultingData!["result"] as IDictionary<string, object>;
            dest.Should().NotBeNull();
            dest.Should()
                .BeEquivalentTo(
                    new Dictionary<string, object?>
                    {
                        ["name"] = "test",
                        ["true"] = true,
                        ["false"] = false,
                        ["null"] = null,
                        ["array"] = new[] { 1, 2, 3 },
                        ["object"] = new Dictionary<string, object> { ["key"] = "value" },
                    }
                );
        }

        [Theory]
        [InlineData(HttpStatusCode.OK, @"{ ""code"": ""OK"" }", true, false, 1)]
        [InlineData(HttpStatusCode.OK, "invalid_json", false, false, 1)]
        [InlineData(HttpStatusCode.BadRequest, @"{ ""field"": ""value"" }", false, true, 1)]
        [InlineData(HttpStatusCode.InternalServerError, "InternalServerError", false, true, 2)]
        [InlineData(HttpStatusCode.Found, "invalid_json", false, true, 1)]
        [InlineData(HttpStatusCode.Found, @"{ ""code"": ""OK"" }", true, true, 1)]
        public void RestTransformation_WithError_ShouldNotRetryAndImmediatelyReturnResult(
            HttpStatusCode httpStatusCode,
            string errorContent,
            bool expectExpandoObjectOnResult,
            bool expectException,
            int repeatCount
        )
        {
            //Arrange
            var source = new MemorySource<ExpandoObject>(new ExpandoObject[] { _fakeSourceData });
            var destination = new MemoryDestination<ExpandoObject>();
            var httpClientMock = CreateMockHttpClient(errorContent, httpStatusCode);
            var trans1 = SetupRestTransformation(httpClientMock.Object, false);
            source.LinkTo(trans1).LinkTo(destination);

            //Act
            source.Execute();
            destination.Wait();

            //Assert
            httpClientMock.Verify(
                x =>
                    x.InvokeAsync(
                        It.IsAny<string>(),
                        It.IsAny<HttpMethod>(),
                        It.IsAny<Dictionary<string, string>>(),
                        It.IsAny<string>()
                    ),
                Times.Exactly(repeatCount)
            );
            var dest = destination.Data?.FirstOrDefault() as IDictionary<string, object?>;
            dest.Should().NotBeNull();
            dest!["http_code"].Should().Be(httpStatusCode);
            dest["raw_response"].Should().Be(errorContent);
            if (expectExpandoObjectOnResult)
            {
                var result = dest!["result"] as IDictionary<string, object?>;
                result.Should().NotBeNull();
                result.Should().BeOfType<ExpandoObject>();
                result!["code"].Should().Be("OK");
            }

            if (expectException)
            {
                dest["exception"].Should().NotBeNull();
                dest["exception"].Should().BeOfType<HttpStatusCodeException>();
                HttpStatusCodeException exception = (HttpStatusCodeException)dest["exception"]!;
                exception.Message.Should().Be(errorContent);
                exception.HttpCode.Should().Be(httpStatusCode);
            }
            else
            {
                dest.Should().NotContainKey("exception");
            }
        }

        [Fact]
        public void RestTransformation_ShouldReturnErrorFieldFromResponse()
        {
            //Arrange
            dynamic data = new ExpandoObject();
            data.urlRouteParameter = "Tom";
            data.urlQueryParameter = 46;
            data.port = 90210;

            var source = new MemorySource<ExpandoObject>(new ExpandoObject[] { data });

            var destination = new MemoryDestination<ExpandoObject>();

            var trans1 = new RestTransformation(
                () =>
                    CreateMockHttpClient(
                        @"{ ""errorCode"": 404, ""errorMessage"": ""NotFound"" }"
                    ).Object
            )
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
                    RetryInterval = 5,
                },
                HttpCodeField = "http_code",
                ResultField = "result",
                ExceptionField = "errorMessage",
                RawResponseField = "raw_response",
                FailOnError = false,
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
            private readonly ConcurrentDictionary<
                (Type type, string? key),
                IConnectionManager
            > _connectionManagers = new();

            public Guid? ReferenceId { get; set; }

            public string Name { get; set; } = null!;

            public int? TimeoutMilliseconds { get; set; }

            public IConnectionManager GetOrAddConnectionManager(
                Type connectionManagerType,
                string? key,
                Func<Type, string?, IConnectionManager> factory
            ) =>
                _connectionManagers.GetOrAdd(
                    (connectionManagerType, key),
                    k => factory(k.type, k.key)
                );

            public IDataFlowSource<ExpandoObject> Source { get; set; } = null!;

            public IList<IDataFlowDestination<ExpandoObject>> Destinations { get; set; } = null!;

            public IList<IDataFlowDestination<ETLBoxError>> ErrorDestinations { get; set; } = null!;

            public XmlSchema? GetSchema() => null;

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
                var tasks = Destinations.Select(d => d.Completion).ToArray();
                Task.WaitAll(tasks);
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this); // Violates rule
            }

            protected virtual void Dispose(bool disposing)
            {
                if (!disposing)
                {
                    return;
                }

                foreach (var value in _connectionManagers.Values)
                {
                    value.Dispose();
                }
            }
        }

        private static Mock<IHttpClient> CreateMockHttpClient(
            string response,
            HttpStatusCode resultCode = HttpStatusCode.OK
        )
        {
            var httpClientMock = new Mock<IHttpClient>();
            if (resultCode == HttpStatusCode.OK)
            {
                httpClientMock
                    .Setup(x =>
                        x.InvokeAsync(
                            It.IsAny<string>(),
                            It.IsAny<HttpMethod>(),
                            It.IsAny<Dictionary<string, string>>(),
                            It.IsAny<string>()
                        )
                    )
                    .ReturnsAsync(() => response);
            }
            else
            {
                httpClientMock
                    .Setup(x =>
                        x.InvokeAsync(
                            It.IsAny<string>(),
                            It.IsAny<HttpMethod>(),
                            It.IsAny<Dictionary<string, string>>(),
                            It.IsAny<string>()
                        )
                    )
                    .Throws(() => new HttpStatusCodeException(resultCode, response));
            }

            return httpClientMock;
        }

        private static RestTransformation SetupRestTransformation(
            IHttpClient httpClient,
            bool failOnError = true
        ) =>
            new(() => httpClient)
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
                    Headers = new Dictionary<string, string>
                    {
                        ["header1"] = "testHeaderValue",
                        ["header2"] = "testHeaderValue2",
                    },
                    Method = "GET",
                    RetryCount = 2,
                    RetryInterval = 0.5,
                },
                ResultField = "result",
                ExceptionField = "exception",
                HttpCodeField = "http_code",
                RawResponseField = "raw_response",
                FailOnError = failOnError,
            };
    }
}
