using System.Collections.Generic;
using System.IO;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using JetBrains.Annotations;
using Newtonsoft.Json;
using TestOtherConnectors.Helpers;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestOtherConnectors.CustomDestination
{
    [Collection("DataFlow")]
    public class CustomDestinationTests
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void InsertIntoTable()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture("CustomDestination");

            //Act
            var source = new DbSource<MySimpleRow>(SqlConnection, "Source");
            var dest = new CustomDestination<MySimpleRow>(row =>
            {
                SqlTask.ExecuteNonQuery(
                    SqlConnection,
                    "Insert row",
                    $"INSERT INTO dbo.CustomDestination VALUES({row.Col1},'{row.Col2}')"
                );
            });
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void CreateJsonFile()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture("JSonSource");
            source2Columns.InsertTestData();
            var source = new DbSource<MySimpleRow>(SqlConnection, "JSonSource");
            var rows = new List<MySimpleRow>();

            //Act
            var dest = new CustomDestination<MySimpleRow>(row => rows.Add(row));
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            //Act
            var json = JsonConvert.SerializeObject(rows, Formatting.Indented);

            //Assert
            Assert.Equal(
                File.ReadAllText("res/CustomDestination/simpleJson_tobe.json")
                    .NormalizeLineEndings(),
                json
            );
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }
    }
}
