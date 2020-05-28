using ALE.ETLBox;
using Xunit;

namespace ALE.ETLBoxTests
{
    public class TableNameDescriptorTests
    {
        [Fact]
        public void SqlServerWithSchema()
        {
            var desc = new ObjectNameDescriptor("dbo.Test", "[", "]");

            Assert.Equal("[dbo]", desc.QuotatedSchemaName);
            Assert.Equal("[Test]", desc.QuotatedObjectName);
            Assert.Equal("[dbo].[Test]", desc.QuotatedFullName);
            Assert.Equal("dbo", desc.UnquotatedSchemaName);
            Assert.Equal("Test", desc.UnquotatedObjectName);
            Assert.Equal("dbo.Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void SqlServerSchemaWithDot()
        {
            var desc = new ObjectNameDescriptor("[Foo.Bar].[Test]", "[", "]");

            Assert.Equal("[Foo.Bar]", desc.QuotatedSchemaName);
            Assert.Equal("[Test]", desc.QuotatedObjectName);
            Assert.Equal("[Foo.Bar].[Test]", desc.QuotatedFullName);
            Assert.Equal("Foo.Bar", desc.UnquotatedSchemaName);
            Assert.Equal("Test", desc.UnquotatedObjectName);
            Assert.Equal("Foo.Bar.Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void SqlServerNoSchema()
        {
            var desc = new ObjectNameDescriptor("Test", "[", "]");

            Assert.Equal("", desc.QuotatedSchemaName);
            Assert.Equal("[Test]", desc.QuotatedObjectName);
            Assert.Equal("[Test]", desc.QuotatedFullName);
            Assert.Equal("Test", desc.UnquotatedObjectName);
            Assert.Equal("Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void SqlServerNoSchemaWithQuotation()
        {
            var desc = new ObjectNameDescriptor("[Test]", "[", "]");

            Assert.Equal("", desc.QuotatedSchemaName);
            Assert.Equal("[Test]", desc.QuotatedObjectName);
            Assert.Equal("[Test]", desc.QuotatedFullName);
            Assert.Equal("Test", desc.UnquotatedObjectName);
            Assert.Equal("Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void PostgresWithSchema()
        {
            var desc = new ObjectNameDescriptor("public.Test", @"""", @"""");

            Assert.Equal(@"""public""", desc.QuotatedSchemaName);
            Assert.Equal(@"""Test""", desc.QuotatedObjectName);
            Assert.Equal(@"""public"".""Test""", desc.QuotatedFullName);
            Assert.Equal(@"Test", desc.UnquotatedObjectName);
            Assert.Equal(@"public.Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void PostgresWithSchemaAndQuotation()
        {
            var desc = new ObjectNameDescriptor(@"""public"".""Test""", @"""", @"""");

            Assert.Equal(@"""public""", desc.QuotatedSchemaName);
            Assert.Equal(@"""Test""", desc.QuotatedObjectName);
            Assert.Equal(@"""public"".""Test""", desc.QuotatedFullName);
            Assert.Equal(@"Test", desc.UnquotatedObjectName);
            Assert.Equal(@"public.Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void MySqlQuotatedTableName()
        {
            var desc = new ObjectNameDescriptor("`Test`", "`", "`");

            Assert.Equal(@"", desc.QuotatedSchemaName);
            Assert.Equal(@"`Test`", desc.QuotatedObjectName);
            Assert.Equal(@"`Test`", desc.QuotatedFullName);
            Assert.Equal(@"Test", desc.UnquotatedObjectName);
            Assert.Equal(@"Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void MySqlQuotatedTableNameAndDots()
        {
            var desc = new ObjectNameDescriptor("`Test.Test`", "`", "`");

            Assert.Equal(@"", desc.QuotatedSchemaName);
            Assert.Equal(@"`Test.Test`", desc.QuotatedObjectName);
            Assert.Equal(@"`Test.Test`", desc.QuotatedFullName);
            Assert.Equal(@"Test.Test", desc.UnquotatedObjectName);
            Assert.Equal(@"Test.Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void PostgresWithSchemaAndDot()
        {
            var desc = new ObjectNameDescriptor(@"""public.dot"".""Test.Test""", @"""", @"""");

            Assert.Equal(@"""public.dot""", desc.QuotatedSchemaName);
            Assert.Equal(@"""Test.Test""", desc.QuotatedObjectName);
            Assert.Equal(@"""public.dot"".""Test.Test""", desc.QuotatedFullName);
            Assert.Equal(@"Test.Test", desc.UnquotatedObjectName);
            Assert.Equal(@"public.dot.Test.Test", desc.UnquotatedFullName);
        }
    }
}
