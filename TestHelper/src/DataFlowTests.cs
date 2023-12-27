using System;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml.Serialization;
using ALE.ETLBox.DataFlow;
using FluentAssertions;
using JetBrains.Annotations;
using TestHelper.Models;

namespace TestHelper
{
    public class DataFlowTests
    {
        [Fact]
        public void DataFlow_ReadFromXml_ShouldBePassed()
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
                                <Uri>{csvUri}</Uri>
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

            step?.Invoke(CancellationToken.None);

            var destinations = step?.Destinations?.Select(d => d as MemoryDestination<ExpandoObject>).ToArray();

            var dest = destinations?.FirstOrDefault();

            //Assert
            step.Should().NotBeNull();
            step!.ReferenceId.Should().Be(referenceId);
            step.Name.Should().Be(name);
            step.TimeoutMilliseconds.Should().Be(ms);

            step.Source.Should().BeOfType<CustomCsvSource>();
            ((CustomCsvSource)step.Source).Uri.Should().Be(csvUri);
            ((CustomCsvSource)step.Source).Guid.Should().Be(referenceId);
            ((CustomCsvSource)step.Source).NullGuid.Should().Be(referenceId);
            ((CustomCsvSource)step.Source).Configuration.Delimiter.Should().Be(";");
            ((CustomCsvSource)step.Source).Configuration.Escape.Should().Be('#');

            dest?.Data.Should().NotBeNull();
            dest!.Data.Should().NotBeEmpty();
            dest.Data.Should().HaveCount(3);

            dynamic col = new ExpandoObject();
            col.Col1 = "1";
            col.Col2 = "Test1";
            dest.Data.Should().ContainEquivalentOf((ExpandoObject)col);
            col.Col1 = "2";
            col.Col2 = "Test2";
            dest.Data.Should().ContainEquivalentOf((ExpandoObject)col);
            col.Col1 = "3";
            col.Col2 = "Test3";
            dest.Data.Should().ContainEquivalentOf((ExpandoObject)col);

            try
            {
                File.Delete(csvUri);
            }
            catch
            {
                // ignore
            }
        }

        [Fact]
        public void DataFlow_TransformationError_ShouldBeHandled()
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
                            <CustomCsvSource>
                                <Uri>{csvUri}</Uri>
                                <Configuration>
                                    <Delimiter>;</Delimiter>
                                    <Escape>#</Escape>
                                    <Quote>$</Quote>
                                </Configuration>
                                <LinkTo>
                                    <BrokenTransformation>
                                        <LinkTo>
                                            <MemoryDestination></MemoryDestination>
                                        </LinkTo>
                                    </BrokenTransformation>
                                </LinkTo>
                            </CustomCsvSource>
		                </EtlDataFlowStep>";

            using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
            var serializer = new XmlSerializer(typeof(EtlDataFlowStep));
            var step = (EtlDataFlowStep)serializer.Deserialize(stream)!;

            step?.Invoke(CancellationToken.None);

            var errors = step?.ErrorDestinations?.Select(d => d as ErrorLogDestination).ToArray();

            Assert.Collection(
                errors[0].Errors,
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

            try
            {
                File.Delete(csvUri);
            }
            catch
            {
                // ignore
            }
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

        private static ExpandoObject CreateObject(string v)
        {
            dynamic obj = new ExpandoObject();
            obj.data = v;
            return obj;
        }

#pragma warning disable S1144 // Unused private types or members should be removed
        [UsedImplicitly]
        private sealed class CustomCsvSource : CsvSource
        {
            public DateTime DateTime { get; set; }
            public DateTime? NullDateTime { get; set; }
            public Guid Guid { get; set; }
            public Guid? NullGuid { get; set; }
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
        }
#pragma warning restore S1144 // Unused private types or members should be removed
    }
}
