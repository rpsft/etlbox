using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [CollectionDefinition("Sql Server Connection Manager")]
    public class CollectionConnectionManagerFixture : ICollectionFixture<ConnectionManagerFixture> { }
    public class ConnectionManagerFixture
    {
        public ConnectionManagerFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnectionString("ConnectionManager").DBName
                , Config.SqlConnectionString("ConnectionManager"));
        }
    }

}
