using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper.DataFlow;
using ETLBox.Primitives;
using ETLBox.Rest;
using ETLBox.Rest.Models;
using FluentAssertions;
using Moq;

namespace TestTransformations.Rest
{
    public class RestTransformationTests
    {
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
                    It.IsAny<Dictionary<string,string>>(),
                    It.IsAny<string>()))
                .ReturnsAsync(() => @"{ ""jsonResponse"" : 100}");

            RestTransformation trans1 = new RestTransformation(() => httpClientMock.Object)
            {
                RestMethodInfo = new RestMethodInfo
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
                    Headers = new() { {"header1", "testHeaderValue"} },
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

            var referenceId = Guid.NewGuid();
            var name = Guid.NewGuid().ToString();
            var ms = 100;
            var xml = @$"<EtlDataFlowStep>
			                <ReferenceId>
                                {referenceId}
                            </ReferenceId>
			                <Name>
                                {name}
                            </Name>
			                <TimeoutMilliseconds>{ms}</TimeoutMilliseconds>
                            <CsvSource>
                                <Uri>{csvUri}</Uri>
                                <Configuration>
                                    <Delimiter>;</Delimiter>
                                    <Escape>#</Escape>
                                    <Quote>$</Quote>
                                </Configuration>
                                <LinkTo>
                                    <TestRestTransformation>
                                        <RestMethodInfo>
                                            <Url>http://test/{{{{urlRouteParameter}}}}?urlQueryParameter={{{{urlQueryParameter}}}}</Url>
                                            <Body>
                                                {{ ""PromoActionApiInternal"": {{
                                                            ""TeasersPath"": ""C:/Loyalty/images/teasers/"",
                                                            ""TeasersUrl"": ""/image/download/"",
                                                            ""ImageDomainUrl"": ""http://localhost:{{{{port}}}}"",
                                                            ""OptInCheckUrlDomainUrl"": ""http://localhost:{{{{port}}}}"",
                                                            ""OptInRelativeUrlTemplate"": ""/Optin/{{0}}/{{1}}/{{2}}""
                                                        }}
                                                }}
                                            </Body>
                                            <Headers>
                                                <header1>testHeaderValue</header1>
                                                <header2>testHeaderValue2</header2>
                                            </Headers>
                                            <Method>
                                                GET
                                            </Method>
                                            <RetryCount>
                                                2
                                            </RetryCount>
                                            <RetryInterval>
                                                5
                                            </RetryInterval>
                                        </RestMethodInfo>
                                        <ResultField>
                                            result
                                        </ResultField>
                                        <LinkTo>
                                            <MemoryDestination></MemoryDestination>
                                        </LinkTo>
                                    </TestRestTransformation>
                                </LinkTo>
                            </CsvSource>
		                </EtlDataFlowStep>";

            using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
            var serializer = new XmlSerializer(typeof(EtlDataFlowStep));
            var step = (EtlDataFlowStep)serializer.Deserialize(stream)!;

            step?.Invoke();

            var destinations = step?.Destinations?.Select(d => d as MemoryDestination<ExpandoObject>).ToArray();

            var destination = destinations.FirstOrDefault();

            var dest = destination?.Data?.FirstOrDefault() as IDictionary<string, object>;

            dest.Should().NotBeNull();
            var res = dest!["result"] as IDictionary<string, object>;

            res.Should().NotBeNull();
            res!["jsonResponse"].Should().Be(100);
        }

        private static string GetCsv()
        {
            var sb = new StringBuilder();
            sb.AppendLine("urlRouteParameter;urlQueryParameter;port");
            sb.AppendLine("Tom;46;90210");
            return sb.ToString();
        }

        private static string CreateFile(string content, string ext)
        {
            var path = Path.Combine(Path.GetTempPath(), $"{Path.GetRandomFileName()}.{ext}");
            File.WriteAllText(path, content);
            return path;
        }

        private static ExpandoObject CreateObject(string v)
        {
            dynamic obj = new ExpandoObject();
            obj.data = v;
            return obj;
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
                this.ReadFromXml(reader);
            }

            public void WriteXml(XmlWriter writer)
            {
                throw new NotImplementedException();
            }

            public void Invoke()
            {
                Source.Execute(CancellationToken.None);
                var tasks = Destinations.Select(d => d.Completion).ToArray();
                Task.WaitAll(tasks);
            }
        }

        public class TestRestTransformation : RestTransformation
        {
            public TestRestTransformation() : base(() => CreateClient())
            {
            }

            private static IHttpClient CreateClient()
            {
                var httpClientMock = new Mock<IHttpClient>();
                httpClientMock
                    .Setup(x => x.InvokeAsync(
                        It.IsAny<string>(),
                        It.IsAny<HttpMethod>(),
                        It.IsAny<Dictionary<string,string>>(),
                        It.IsAny<string>()))
                    .ReturnsAsync(() => @"{ ""jsonResponse"" : 100}");

                return httpClientMock.Object;
            }
        }
    }
}
