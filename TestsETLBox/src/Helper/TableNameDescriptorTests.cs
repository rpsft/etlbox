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
            TableNameDescriptor desc = new TableNameDescriptor("dbo.Test", ConnectionManagerType.SqlServer);

            Assert.Equal("[dbo]", desc.QuotatedSchemaName);
            Assert.Equal("[Test]", desc.QuotatedTableName);
            Assert.Equal("[dbo].[Test]", desc.QuotatedFullName);
        }

        [Fact]
        public void SqlServerNoSchema()
        {
            TableNameDescriptor desc = new TableNameDescriptor("Test", ConnectionManagerType.SqlServer);

            Assert.Equal("", desc.QuotatedSchemaName);
            Assert.Equal("[Test]", desc.QuotatedTableName);
            Assert.Equal("[Test]", desc.QuotatedFullName);
        }

        [Fact]
        public void PostgresWithSchema()
        {
            TableNameDescriptor desc = new TableNameDescriptor("public.Test", ConnectionManagerType.Postgres);

            Assert.Equal(@"""public""", desc.QuotatedSchemaName);
            Assert.Equal(@"""Test""", desc.QuotatedTableName);
            Assert.Equal(@"""public"".""Test""", desc.QuotatedFullName);
        }

        [Fact]
        public void MySqlQuotatedTableName()
        {
            TableNameDescriptor desc = new TableNameDescriptor("`Test`", ConnectionManagerType.MySql);

            Assert.Equal(@"", desc.QuotatedSchemaName);
            Assert.Equal(@"`Test`", desc.QuotatedTableName);
            Assert.Equal(@"`Test`", desc.QuotatedFullName);
        }
    }
}
