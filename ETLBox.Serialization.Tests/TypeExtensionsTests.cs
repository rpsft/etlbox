using System;
using System.Globalization;
using ALE.ETLBox.Serialization.DataFlow;
using FluentAssertions;

namespace ETLBox.Serialization.Tests
{
    public sealed class TypeExtensionsTests
    {
        [Theory]
        [InlineData(typeof(string), "1")]
        [InlineData(typeof(char), '1')]
        [InlineData(typeof(byte), (byte)1)]
        [InlineData(typeof(short), (short)1)]
        [InlineData(typeof(ushort?), (ushort)1)]
        [InlineData(typeof(int), 1)]
        [InlineData(typeof(int?), 1)]
        [InlineData(typeof(uint), (uint)1)]
        [InlineData(typeof(uint?), (uint)1)]
        [InlineData(typeof(long), (long)1)]
        [InlineData(typeof(long?), (long)1)]
        [InlineData(typeof(ulong), (ulong)1)]
        [InlineData(typeof(ulong?), (ulong)1)]
        [InlineData(typeof(double), (double)1)]
        public void GetValue_ObjectValueShouldBeReturned(Type type, object? value)
        {
            var result = "1".TryParse(type, out var obj);
            result.Should().BeTrue();
            obj.Should().Be(value);
        }

        [Theory]
        [InlineData(typeof(Guid))]
        [InlineData(typeof(Guid?))]
        public void GetValue_WithGuid_ObjectValueShouldBeReturned(Type type)
        {
            var id = Guid.Parse("db31477f-1b02-4e82-a14e-fa26a35a27da");

            var result = "db31477f-1b02-4e82-a14e-fa26a35a27da".TryParse(type, out var obj);

            result.Should().BeTrue();
            obj.Should().Be(id);
        }

        [Theory]
        [InlineData(typeof(DateTime))]
        [InlineData(typeof(DateTime?))]
        public void GetValue_WithDateTime_ObjectValueShouldBeReturned(Type type)
        {
            var now = new DateTime(2024,1,2,3,4, 0, DateTimeKind.Local);
            var date = now.ToString(CultureInfo.InvariantCulture);

            var result = date.TryParse(type, out var obj);

            result.Should().BeTrue();
            obj.Should().Be(now);
        }

        [Fact]
        public void GetValue_NullValueShouldBeReturned()
        {
            var result = "1".TryParse(typeof(UnsupportedType), out var obj);

            result.Should().BeFalse();
            obj.Should().BeNull();
        }

        public struct UnsupportedType
        {
            // test
        }
    }
}
