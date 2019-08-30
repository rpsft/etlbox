using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [CollectionDefinition("Sql Server DataFlow Source and Destination")]
    public class DatalFlowSourceDestinationCollectionClass : ICollectionFixture<DatabaseSourceDestinationFixture> { }
    public class DatabaseSourceDestinationFixture
    {
        public DatabaseSourceDestinationFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnectionString("DataFlowSource").DBName
                , Config.SqlConnectionString("DataFlowSource"));
            DatabaseHelper.RecreateDatabase(Config.SqlConnectionString("DataFlowDestination").DBName
                , Config.SqlConnectionString("DataFlowDestination"));
        }
    }

}
