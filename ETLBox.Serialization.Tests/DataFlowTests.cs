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
                        <Double>1.0</Double>
                        <NullInt>-1</NullInt>
                        <NullUint>1</NullUint>
                        <NullLong>-1</NullLong>
                        <NullUlong>1</NullUlong>
                        <NullShort>-1</NullShort>
                        <NullUshort>1</NullUshort>
                        <NullDouble>-1.0</NullDouble>
                        <Uri>{_csvUri}</Uri>
                        <Strings>
                            <string>test</string>
                            <string><![CDATA[test<!"""">]]></string>
                            <string><![CDATA[test1<!"""">]]><![CDATA[test2<!"""">]]></string>
                        </Strings>
                        <Stream type=""MemoryStream"" />
                        <Enum>Value2</Enum>
                        <NullEnum>Value1</NullEnum>
                        <IntegerList>
                            <int>1</int>
                            <int>2</int>
                            <int>3</int>
                        </IntegerList>
                        <Configuration>
                            <Delimiter>;</Delimiter>
                            <Escape>#</Escape>
                            <Quote>$</Quote>
                        </Configuration>
                        <Metadata>
                            <Source>TestSystem</Source>
                            <Version>1.0</Version>
                            <Settings>
                                <Timeout>30</Timeout>
                                <Retries>3</Retries>
                            </Settings>
                        </Metadata>
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

            var strings = customCsvSource.Strings.ToArray();
            strings.Should().HaveCount(3);
            strings[0].Should().Be("test");
            strings[1].Should().Be("test<!\"\">");
            strings[2].Should().Be("test1<!\"\">test2<!\"\">");
            customCsvSource.Stream.Should().NotBeNull();
            customCsvSource.Stream.Should().BeOfType<MemoryStream>();
            customCsvSource.Enum.Should().Be(CustomCsvSource.EnumType.Value2);
            customCsvSource.NullEnum.Should().Be(CustomCsvSource.EnumType.Value1);
            customCsvSource.IntegerList.Should().NotBeNullOrEmpty();
            customCsvSource.IntegerList.Should().BeEquivalentTo(new[] { 1, 2, 3 });

            // Verify Metadata (IDictionary<string, object?>) deserialization
            customCsvSource.Metadata.Should().NotBeNull();
            customCsvSource.Metadata.Should().HaveCount(3);
            customCsvSource.Metadata.Should().ContainKey("Source");
            customCsvSource.Metadata.Should().ContainKey("Version");
            customCsvSource.Metadata.Should().ContainKey("Settings");
            customCsvSource.Metadata!["Source"].Should().Be("TestSystem");
            customCsvSource.Metadata["Version"].Should().Be("1.0");

            var settings = customCsvSource.Metadata["Settings"] as IDictionary<string, object?>;
            settings.Should().NotBeNull();
            settings!.Should().ContainKey("Timeout");
            settings.Should().ContainKey("Retries");
            settings!["Timeout"].Should().Be("30");
            settings["Retries"].Should().Be("3");

            customCsvSource.Char.Should().Be('#');
            customCsvSource.Byte.Should().Be(1);
            customCsvSource.Int.Should().Be(-1);
            customCsvSource.Uint.Should().Be(1);
            customCsvSource.NullInt.Should().Be(-1);
            customCsvSource.Long.Should().Be(-1);
            customCsvSource.Ulong.Should().Be(1);
            customCsvSource.NullLong.Should().Be(-1);
            customCsvSource.Double.Should().Be(1.0);
            customCsvSource.NullDouble.Should().Be(-1.0);
            customCsvSource.NullDouble.Should().BeOfType(typeof(double));

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
        public void DataFlow_ReadFromXml_NotSetNullableShouldBeProcessed()
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
                        <NullBool/>
                        <NullChar/>
                        <NullByte/>
                        <NullDateTime/>
                        <NullGuid/>
                        <NullInt/>
                        <NullUint/>
                        <NullLong/>
                        <NullUlong/>
                        <NullShort/>
                        <NullUshort/>
                        <NullDouble/>
                        <NullEnum/>
                        <LinkTo>
                            <MemoryDestination/>
                        </LinkTo>
                    </CustomCsvSource>
		        </EtlDataFlowStep>";

            using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
            var serializer = new XmlSerializer(typeof(EtlDataFlowStep));
            var step = (EtlDataFlowStep)serializer.Deserialize(stream)!;

            //Assert
            step.Should().NotBeNull();
            step.ReferenceId.Should().Be(referenceId);
            step.Name.Should().Be(name);

            step.Source.Should().BeOfType<CustomCsvSource>();
            var customCsvSource = (CustomCsvSource)step.Source;
            customCsvSource.NullBool.Should().BeNull();
            customCsvSource.NullBool.HasValue.Should().BeFalse();
            customCsvSource.NullChar.Should().BeNull();
            customCsvSource.NullChar.HasValue.Should().BeFalse();
            customCsvSource.NullByte.Should().BeNull();
            customCsvSource.NullByte.HasValue.Should().BeFalse();
            customCsvSource.NullDateTime.Should().BeNull();
            customCsvSource.NullDateTime.HasValue.Should().BeFalse();
            customCsvSource.NullGuid.Should().BeNull();
            customCsvSource.NullGuid.HasValue.Should().BeFalse();
            customCsvSource.NullEnum.Should().BeNull();
            customCsvSource.NullEnum.HasValue.Should().BeFalse();
            customCsvSource.NullShort.Should().BeNull();
            customCsvSource.NullShort.HasValue.Should().BeFalse();
            customCsvSource.NullUshort.Should().BeNull();
            customCsvSource.NullUshort.HasValue.Should().BeFalse();
            customCsvSource.NullInt.Should().BeNull();
            customCsvSource.NullInt.HasValue.Should().BeFalse();
            customCsvSource.NullUint.Should().BeNull();
            customCsvSource.NullUint.HasValue.Should().BeFalse();
            customCsvSource.NullLong.Should().BeNull();
            customCsvSource.NullLong.HasValue.Should().BeFalse();
            customCsvSource.NullUlong.Should().BeNull();
            customCsvSource.NullUlong.HasValue.Should().BeFalse();
            customCsvSource.NullDouble.Should().BeNull();
            customCsvSource.NullDouble.HasValue.Should().BeFalse();
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

        [Fact]
        public void DataFlow_AITransformation_ShouldBePassed()
        {
            // Arrange
            var referenceId = Guid.NewGuid();
            var name = Guid.NewGuid().ToString();
            var xml =
                @$"<EtlDataFlowStep>
			        <ReferenceId>
                        {referenceId}
                    </ReferenceId>
			        <Name>
                        {name}
                    </Name>
			        <TimeoutMilliseconds>100</TimeoutMilliseconds>
                    <CustomCsvSource>
                        <Uri>{_csvUri}</Uri>
                        <Configuration>
                            <Delimiter>;</Delimiter>
                            <Escape>#</Escape>
                            <Quote>$</Quote>
                        </Configuration>
                        <LinkTo>
                            <AIBatchTransformation>
						<BatchSize>10</BatchSize>
						<ApiSettings>
							<ApiKey>ApiKey</ApiKey>
							<ApiType>openai</ApiType>
							<ApiModel>ApiModel</ApiModel>
							<ApiBaseUrl>https://localhost</ApiBaseUrl>
						</ApiSettings>
						<FailOnError>false</FailOnError>
						<ResultSettings>
						<ResultItemsJsonPath>results</ResultItemsJsonPath>
						<ResultField>Result</ResultField>
						<RawResponseField>Response</RawResponseField>
						<ExceptionField>Exception</ExceptionField>
						<HttpCodeField>HttpCode</HttpCodeField>
						<InputDataIdentificationField>respondent_id</InputDataIdentificationField>
						<ResultDataIdentificationField>respondent_id</ResultDataIdentificationField>
						<ResultsJsonSchema></ResultsJsonSchema>
						</ResultSettings>
                        <PromptParameters>
                          <ReasonsList>12345</ReasonsList>
                          <MaxTokens>1000</MaxTokens>
                          <Temperature>0.7</Temperature>
                          <NestedConfig>
                            <Model>gpt-4</Model>
                            <Version>v1</Version>
                          </NestedConfig>
                        </PromptParameters>
						<Prompt>Prompt</Prompt>
                                <LinkTo>
                                    <MemoryDestination>
                                    </MemoryDestination>
                                </LinkTo>
                                <LinkErrorTo>
                                    <ErrorLogDestination/>
                                </LinkErrorTo>
                            </AIBatchTransformation>
                        </LinkTo>
                    </CustomCsvSource>
		        </EtlDataFlowStep>";

            using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
            var serializer = new XmlSerializer(typeof(EtlDataFlowStep));

            // Act
            var step = (EtlDataFlowStep)serializer.Deserialize(stream)!;
            step.Invoke(CancellationToken.None);

            // Assert - Verify basic deserialization
            step.Source.Should().NotBeNull().And.BeOfType<CustomCsvSource>();
            step.Destinations.Should().NotBeNull().And.HaveCountGreaterThan(0);

            // Verify ErrorDestinations structure
            step.ErrorDestinations.Should().NotBeNull();
            step.ErrorDestinations.Should().HaveCount(1);
            step.ErrorDestinations[0].Should().BeOfType<ErrorLogDestination>();
        }

        [Theory]
        [InlineData(nameof(MemoryDestination))]
        [InlineData(nameof(DbDestination))]
        [InlineData(nameof(CsvDestination))]
        [InlineData(nameof(CustomCsvDestination<string>))]
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
            EtlDataFlowStep step = DataFlowXmlReader.Deserialize<EtlDataFlowStep>(
                xml,
                errorLogDestination
            );

            // Assert
            step.ErrorDestinations.Should().NotBeNull();
            step.ErrorDestinations.Should().HaveCount(1);
            step.ErrorDestinations.Should().AllBeAssignableTo<ErrorLogDestination>();
        }

        [Fact]
        public void DataFlow_ReadFromXml_CheckConnectionManagerToDispose()
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
                    <MemorySource>
                        <ConnectionManager type=""PostgresConnectionManager"">
                            <ConnectionString type=""PostgresConnectionString"">
                                <Value>Server=local;Port=123;Database=test;User ID=userId;Password=secret;</Value>
                            </ConnectionString>
                        </ConnectionManager>
                        <LinkTo>
                            <MemoryDestination />
                        </LinkTo>
                    </MemorySource>
		        </EtlDataFlowStep>";

            using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
            var serializer = new XmlSerializer(typeof(EtlDataFlowStep));
            using var step = (EtlDataFlowStep)serializer.Deserialize(stream)!;

            step.Invoke(CancellationToken.None);

            //Assert
            step.Should().NotBeNull();
            step.ReferenceId.Should().Be(referenceId);
            step.Name.Should().Be(name);
            step.ConnectionManagerCount().Should().Be(1);
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
            public DateTime? NullDateTime { get; set; } = null;
            public Guid Guid { get; set; } = Guid.Empty;
            public Guid? NullGuid { get; set; } = null;
            public char Char { get; set; } = (char)0;
            public char? NullChar { get; set; } = null;
            public byte Byte { get; set; } = 0;
            public byte? NullByte { get; set; } = null;
            public bool Bool { get; set; }
            public bool? NullBool { get; set; } = null;
            public int Int { get; set; } = 0;
            public int? NullInt { get; set; } = null;
            public uint Uint { get; set; } = 0;
            public uint? NullUint { get; set; } = null;
            public long Long { get; set; } = 0;
            public long? NullLong { get; set; } = null;
            public ulong Ulong { get; set; } = 0;
            public ulong? NullUlong { get; set; } = null;
            public short Short { get; set; }
            public short? NullShort { get; set; } = null;
            public ushort Ushort { get; set; }
            public ushort? NullUshort { get; set; } = null;
            public double Double { get; set; } = 0;
            public double? NullDouble { get; set; } = null;
            public IEnumerable<string> Strings { get; set; } = null!;
            public Stream Stream { get; set; } = null!;
            public EnumType Enum { get; set; } = EnumType.Value1;
            public EnumType? NullEnum { get; set; } = null;
            public List<int> IntegerList { get; set; } = null!;
            public IDictionary<string, object?>? Metadata { get; set; } = null;

            public enum EnumType
            {
                Value1 = 1,
                Value2 = 2,
            }
        }

        private sealed class CustomCsvDestination<T> : CsvDestination<T> { }

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
