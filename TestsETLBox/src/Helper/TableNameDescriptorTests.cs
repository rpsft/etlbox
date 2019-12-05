using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests
{
    public class TableNameDescriptorTests
    {
        [Fact]
        public void SqlServerWithSchema()
        {
            ObjectNameDescriptor desc = new ObjectNameDescriptor("dbo.Test", ConnectionManagerType.SqlServer);

            Assert.Equal("[dbo]", desc.QuotatedSchemaName);
            Assert.Equal("[Test]", desc.QuotatedObjectName);
            Assert.Equal("[dbo].[Test]", desc.QuotatedFullName);
            Assert.Equal("dbo", desc.UnquotatedSchemaName);
            Assert.Equal("Test", desc.UnquotatedObjectName);
            Assert.Equal("dbo.Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void SqlServerNoSchema()
        {
            ObjectNameDescriptor desc = new ObjectNameDescriptor("Test", ConnectionManagerType.SqlServer);

            Assert.Equal("", desc.QuotatedSchemaName);
            Assert.Equal("[Test]", desc.QuotatedObjectName);
            Assert.Equal("[Test]", desc.QuotatedFullName);
            Assert.Equal("Test", desc.UnquotatedObjectName);
            Assert.Equal("Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void PostgresWithSchema()
        {
            ObjectNameDescriptor desc = new ObjectNameDescriptor("public.Test", ConnectionManagerType.Postgres);

            Assert.Equal(@"""public""", desc.QuotatedSchemaName);
            Assert.Equal(@"""Test""", desc.QuotatedObjectName);
            Assert.Equal(@"""public"".""Test""", desc.QuotatedFullName);
            Assert.Equal(@"Test", desc.UnquotatedObjectName);
            Assert.Equal(@"public.Test", desc.UnquotatedFullName);
        }

        [Fact]
        public void MySqlQuotatedTableName()
        {
            ObjectNameDescriptor desc = new ObjectNameDescriptor("`Test`", ConnectionManagerType.MySql);

            Assert.Equal(@"", desc.QuotatedSchemaName);
            Assert.Equal(@"`Test`", desc.QuotatedObjectName);
            Assert.Equal(@"`Test`", desc.QuotatedFullName);
            Assert.Equal(@"Test", desc.UnquotatedObjectName);
            Assert.Equal(@"Test", desc.UnquotatedFullName);
        }

    }
}
