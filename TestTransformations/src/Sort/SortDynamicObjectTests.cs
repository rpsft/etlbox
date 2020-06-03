using ETLBox.Connection;
using ETLBox.DataFlow; using ETLBox.DataFlow.Connectors; using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class SortDynamicObjectTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public SortDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SortSimpleDataDescending()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("SortSourceNonGeneric");
            source2Columns.InsertTestData();
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(Connection, "SortSourceNonGeneric");

            //Act
            List<ExpandoObject> actual = new List<ExpandoObject>();
            CustomDestination<ExpandoObject> dest = new CustomDestination<ExpandoObject>(
                row => actual.Add(row)
            );
            Comparison<ExpandoObject> comp = new Comparison<ExpandoObject>(
                   (x, y) =>
                   {
                       dynamic xo = x as ExpandoObject;
                       dynamic yo = y as ExpandoObject;
                       return yo.Col1 - xo.Col1;
                   }
                );
            Sort<ExpandoObject> block = new Sort<ExpandoObject>(comp);
            source.LinkTo(block);
            block.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            List<int> expected = new List<int>() { 3, 2, 1 };
            Assert.Equal(expected, actual.Select(row => { dynamic r = row as ExpandoObject; return r.Col1; }).Cast<int>().ToList());
        }
    }
}
