using System.Dynamic;
using System.Text;
using System.Xml.Serialization;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Serialization.DataFlow;
using FluentAssertions;
using JetBrains.Annotations;

namespace ETLBox.Serialization.Tests
{
    public sealed class DataFlowTests : IDisposable
    {
        private readonly string _csvUri;

        public DataFlowTests()
        {
            var csv = GetCsv();
            _csvUri = CreateFile(csv, "csv");
        }

        [Fact]
        public void DataFlow_ReadFromXml_ShouldBePassed()
        {
            var referenceId = Guid.NewGuid();
            var name = Guid.NewGuid().ToString();
            const int ms = 100;
            var xml =
                @$"<EtlDataFlowStep>
			        <ReferenceId>
                        {referenceId}
                    </ReferenceId>
			        <Name>
                        {name}
                    </Name>
			        <TimeoutMilliseconds>{ms}</TimeoutMilliseconds>
                    <CustomCsvSource>
                        <Bool>True</Bool>
                        <Char>#</Char>
                        <Byte>1</Byte>
                        <DateTime>11.07.1976</DateTime>
                        <Guid>{referenceId}</Guid>
                        <NullBool>True</NullBool>
                        <NullChar>#</NullChar>
                        <NullByte>1</NullByte>
                        <NullDateTime>11.07.1976</NullDateTime>
                        <NullGuid>{referenceId}</NullGuid>
                        <Int>-1</Int>
                        <Uint>1</Uint>
                        <Long>-1</Long>
                        <Ulong>1</Ulong>
                        <Short>-1</Short>
                        <Ushort>1</Ushort>
                        <NullInt>-1</NullInt>
                        <NullUint>1</NullUint>
                        <NullLong>-1</NullLong>
                        <NullUlong>1</NullUlong>
                        <NullShort>-1</NullShort>
                        <NullUshort>1</NullUshort>
                        <Uri>{_csvUri}</Uri>
                        <Strings>
                            <string>test</string>
                        </Strings>
                        <Configuration>
                            <Delimiter>;</Delimiter>
                            <Escape>#</Escape>
                            <Quote>$</Quote>
                        </Configuration>
                        <LinkTo>
                            <JsonTransformation>
                                <Mappings>
                                    <Col1>
                                        <Name>data</Name>
                                        <Path>$.Data.Id</Path>
                                    </Col1>
                                    <Col2>
                                        <Name>data</Name>
                                        <Path>$.Data.Name</Path>
                                    </Col2>
                                </Mappings>
                                <LinkTo>
                                    <MemoryDestination></MemoryDestination>
                                </LinkTo>
                            </JsonTransformation>
                        </LinkTo>
                    </CustomCsvSource>
		        </EtlDataFlowStep>";

            using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
            var serializer = new XmlSerializer(typeof(EtlDataFlowStep));
            var step = (EtlDataFlowStep)serializer.Deserialize(stream)!;

            step.Invoke(CancellationToken.None);

            //Assert
            step.Should().NotBeNull();
            step.ReferenceId.Should().Be(referenceId);
            step.Name.Should().Be(name);
            step.TimeoutMilliseconds.Should().Be(ms);

            step.Source.Should().BeOfType<CustomCsvSource>();
            var customCsvSource = (CustomCsvSource)step.Source;
            customCsvSource.Uri.Should().Be(_csvUri);
            customCsvSource.Guid.Should().Be(referenceId);
            customCsvSource.NullGuid.Should().Be(referenceId);
            customCsvSource.Configuration.Delimiter.Should().Be(";");
            customCsvSource.Configuration.Escape.Should().Be('#');
            customCsvSource.Strings.Should().NotBeNullOrEmpty();
            customCsvSource.Strings.Should().HaveCount(1);
            customCsvSource.Strings!.First().Should().Be("test");

            step.Destinations.Should().NotBeNull();
            step.Destinations.Should().NotBeEmpty();
            step.Destinations.Should().AllBeAssignableTo<MemoryDestination<ExpandoObject>>();
            step.Destinations.Should().HaveCount(1);

            var dest = (MemoryDestination<ExpandoObject>)step.Destinations[0];
            dest.Data.Should().NotBeNull();
            dest.Data.Should().NotBeEmpty();
            dest.Data.Should().HaveCount(3);

            dynamic col = new ExpandoObject();
            col.Col1 = 1;
            col.Col2 = "Test1";
            dest.Data.Should().ContainEquivalentOf((ExpandoObject)col);
            col.Col1 = 2;
            col.Col2 = "Test2";
            dest.Data.Should().ContainEquivalentOf((ExpandoObject)col);
            col.Col1 = 3;
            col.Col2 = "Test3";
            dest.Data.Should().ContainEquivalentOf((ExpandoObject)col);
        }

        [Fact]
        public void DataFlow_TransformationError_ShouldBeHandled()
        {
            // Arrange
            var referenceId = Guid.NewGuid();
            var name = Guid.NewGuid().ToString();
            const int ms = 100;
            var xml =
                @$"<EtlDataFlowStep>
			        <ReferenceId>
                        {referenceId}
                    </ReferenceId>
			        <Name>
                        {name}
                    </Name>
			        <TimeoutMilliseconds>{ms}</TimeoutMilliseconds>
                    <CustomCsvSource>
                        <Uri>{_csvUri}</Uri>
                        <Configuration>
                            <Delimiter>;</Delimiter>
                            <Escape>#</Escape>
                            <Quote>$</Quote>
                        </Configuration>
                        <LinkTo>
                            <BrokenTransformation>
                                <LinkTo>
                                    <MemoryDestination>
                                    </MemoryDestination>
                                </LinkTo>
                                <LinkErrorTo>
                                    <ErrorLogDestination/>
                                </LinkErrorTo>
                            </BrokenTransformation>
                        </LinkTo>
                    </CustomCsvSource>
		        </EtlDataFlowStep>";

            using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
            var serializer = new XmlSerializer(typeof(EtlDataFlowStep));

            // Act
            var step = (EtlDataFlowStep)serializer.Deserialize(stream)!;
            step.Invoke(CancellationToken.None);

            // Assert
            step.ErrorDestinations.Should().NotBeNull();
            step.ErrorDestinations.Should().HaveCount(1);
            step.ErrorDestinations.Should().AllBeAssignableTo<ErrorLogDestination>();

            var errorDestination = (ErrorLogDestination)step.ErrorDestinations[0];

            Assert.Collection(
                errorDestination.Errors,
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    ),
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    ),
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    )
            );
        }

        [Theory]
        [InlineData("MemoryDestination")]
        [InlineData("DbDestination")]
        [InlineData("CsvDestination")]
        public void DataFlow_Deserialize_ShouldReturnErrorDestinations_NotEmpty(string dest)
        {
            // Arrange
            var xml =
                @$"<EtlDataFlowStep>
                    <CustomCsvSource>
                        <LinkTo>
                            <{dest}>
                                <LinkErrorTo>
                                    <ErrorLogDestination/>
                                </LinkErrorTo>
                            </{dest}>
                        </LinkTo>
                    </CustomCsvSource>
		        </EtlDataFlowStep>";

            using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
            var serializer = new XmlSerializer(typeof(EtlDataFlowStep));

            // Act
            var step = (EtlDataFlowStep)serializer.Deserialize(stream)!;

            // Assert
            step.ErrorDestinations.Should().NotBeNull();
            step.ErrorDestinations.Should().HaveCount(1);
            step.ErrorDestinations.Should().AllBeAssignableTo<ErrorLogDestination>();
        }

        [Fact]
        public void Should_LinkAllErrors()
        {
            // Arrange
            var xml =
                @"<EtlDataFlowStep>
                    <CustomCsvSource>
                    </CustomCsvSource>
		        </EtlDataFlowStep>";
            var errorLogDestination = new ErrorLogDestination();

            // Act
            EtlDataFlowStep step = DataFlowXmlReader.Deserialize<EtlDataFlowStep>(xml, errorLogDestination);

            // Assert
            step.ErrorDestinations.Should().NotBeNull();
            step.ErrorDestinations.Should().HaveCount(1);
            step.ErrorDestinations.Should().AllBeAssignableTo<ErrorLogDestination>();
        }

        private static string GetCsv()
        {
            var builder = new StringBuilder();
            builder.AppendLine("data");
            builder.AppendLine(@"{""Data"": {""Id"": 1, ""Name"": ""Test1""}}");
            builder.AppendLine(@"{""Data"": {""Id"": 2, ""Name"": ""Test2""}}");
            builder.AppendLine(@"{""Data"": {""Id"": 3, ""Name"": ""Test3""}}");
            return builder.ToString();
        }

        private static string CreateFile(string content, string ext)
        {
            var path = Path.Combine(Path.GetTempPath(), $"{Path.GetRandomFileName()}.{ext}");
            File.WriteAllText(path, content);
            return path;
        }

#pragma warning disable S1144 // Unused private types or members should be removed
        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private sealed class CustomCsvSource : CsvSource
        {
            public DateTime DateTime { get; set; }
            public DateTime? NullDateTime { get; set; }
            public Guid Guid { get; set; } = Guid.Empty;
            public Guid? NullGuid { get; set; } = null;
            public char Char { get; set; }
            public char? NullChar { get; set; }
            public byte Byte { get; set; }
            public byte? NullByte { get; set; }
            public bool Bool { get; set; }
            public bool? NullBool { get; set; }
            public int Int { get; set; }
            public int? NullInt { get; set; }
            public uint Uint { get; set; }
            public uint? NullUint { get; set; }
            public long Long { get; set; }
            public long? NullLong { get; set; }
            public ulong Ulong { get; set; }
            public ulong? NullUlong { get; set; }
            public short Short { get; set; }
            public short? NullShort { get; set; }
            public ushort Ushort { get; set; }
            public ushort? NullUshort { get; set; }
            public IEnumerable<string> Strings { get; set; } = null!;
        }
#pragma warning restore S1144 // Unused private types or members should be removed

        public void Dispose()
        {
            try
            {
                File.Delete(_csvUri);
            }
            catch
            {
                // ignore
            }
        }
    }
}
