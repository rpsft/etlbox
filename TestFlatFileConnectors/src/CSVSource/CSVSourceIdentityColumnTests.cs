using System.Globalization;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.CSVSource
{
    [Collection("DataFlow")]
    public class CsvSourceIdentityColumnTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void IdentityAtPosition1()
        {
            var saveCulture = CultureInfo.CurrentCulture;
            try
            {
                //Arrange
                var dest4Columns = new FourColumnsTableFixture(
                    "CsvDestination4Columns",
                    identityColumnIndex: 0
                );
                var dest = new DbDestination<string[]>(Connection, "CsvDestination4Columns");
                var source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");

                if (source.CurrentCulture != null)
                    CultureInfo.CurrentCulture = source.CurrentCulture;

                //Act
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();

                //Assert
                dest4Columns.AssertTestData();
            }
            finally
            {
                CultureInfo.CurrentCulture = saveCulture;
            }
        }

        [Fact]
        public void IdentityInTheMiddle()
        {
            var saveCulture = CultureInfo.CurrentCulture;
            try
            {
                //Arrange
                var dest4Columns = new FourColumnsTableFixture(
                    "CsvDestination4Columns",
                    identityColumnIndex: 2
                );
                var dest = new DbDestination<string[]>(Connection, "CsvDestination4Columns");
                var source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
                if (source.CurrentCulture != null)
                    CultureInfo.CurrentCulture = source.CurrentCulture;

                //Act
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();

                //Assert
                dest4Columns.AssertTestData();
            }
            finally
            {
                CultureInfo.CurrentCulture = saveCulture;
            }
        }

        [Fact]
        public void IdentityAtTheEnd()
        {
            var saveCulture = CultureInfo.CurrentCulture;
            try
            {
                //Arrange
                CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
                var dest4Columns = new FourColumnsTableFixture(
                    "CsvDestination4Columns",
                    identityColumnIndex: 3
                );
                var dest = new DbDestination<string[]>(Connection, "CsvDestination4Columns");

                //Act
                var source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();

                //Assert
                dest4Columns.AssertTestData();
            }
            finally
            {
                CultureInfo.CurrentCulture = saveCulture;
            }
        }
    }
}
