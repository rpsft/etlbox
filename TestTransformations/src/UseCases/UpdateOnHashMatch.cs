using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using TestShared.Helper;

namespace TestTransformations.UseCases
{
    [Collection("DataFlow")]
    public class UpdateOnHashMatchTests
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        private void CreateSourceTable(string tableName)
        {
            DropTableTask.DropIfExists(SqlConnection, tableName);
            TableDefinition sourceTable = new TableDefinition(
                tableName,
                new List<TableColumn>
                {
                    new("id", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
                    new("name", "NVARCHAR(100)", allowNulls: false),
                    new("age", "INT", allowNulls: false),
                }
            );
            sourceTable.CreateTable(SqlConnection);
            SqlTask.ExecuteNonQuery(
                SqlConnection,
                "Insert demo data",
                $"INSERT INTO {tableName} (name, age) VALUES('Bugs',12)"
            );
            SqlTask.ExecuteNonQuery(
                SqlConnection,
                "Insert demo data",
                $"INSERT INTO {tableName} (name, age) VALUES('Coyote',8)"
            );
            SqlTask.ExecuteNonQuery(
                SqlConnection,
                "Insert demo data",
                $"INSERT INTO {tableName} (name, age) VALUES('Pete',19)"
            );
        }

        private void CreateDestinationTable(string tableName)
        {
            DropTableTask.DropIfExists(SqlConnection, tableName);
            TableDefinition sourceTable = new TableDefinition(
                tableName,
                new List<TableColumn>
                {
                    new("id", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
                    new("name", "NVARCHAR(100)", allowNulls: false),
                    new("age", "INT", allowNulls: false),
                    new("hashcode", "CHAR(40)", allowNulls: false),
                }
            );
            sourceTable.CreateTable(SqlConnection);
            SqlTask.ExecuteNonQuery(
                SqlConnection,
                "Insert demo data",
                $"INSERT INTO {tableName} (name, age, hashcode) VALUES('Bugs',12, '{HashHelper.Encrypt_Char40("1Bugs12")}')"
            );
            SqlTask.ExecuteNonQuery(
                SqlConnection,
                "Insert demo data",
                $"INSERT INTO {tableName} (name, age, hashcode) VALUES('Coyote',10, '{HashHelper.Encrypt_Char40("2Coyote10")}')"
            );
        }

        [Fact]
        public void UpdateOnHashMatch()
        {
            //Arrange
            CreateSourceTable("dbo.HashMatchSource");
            CreateDestinationTable("dbo.HashMatchDestination");

            //Act
            DbSource<string[]> source = new DbSource<string[]>(
                SqlConnection,
                "dbo.HashMatchSource"
            );

            RowTransformation<string[]> trans = new RowTransformation<string[]>(row =>
            {
                Array.Resize(ref row, row.Length + 1);
                row[row.Length - 1] = HashHelper.Encrypt_Char40(string.Join("", row));
                return row;
            });

            List<string[]> allEntriesInDestination = new List<string[]>();
            LookupTransformation<string[], string[]> lookup = new LookupTransformation<
                string[],
                string[]
            >(
                new DbSource<string[]>(SqlConnection, "dbo.HashMatchDestination"),
                row =>
                {
                    var matchingIdEntry = allEntriesInDestination
                        .Where(destRow => destRow[0] == row[0])
                        .FirstOrDefault();
                    if (matchingIdEntry == null)
                        row = null;
                    else if (matchingIdEntry[matchingIdEntry.Length - 1] != row[row.Length - 1])
                    {
                        SqlTask.ExecuteNonQuery(
                            SqlConnection,
                            "update entry with different hashcode",
                            $@"UPDATE dbo.HashMatchDestination 
                                                  SET name = '{row[1]}',
                                                      age = '{row[2]}',
                                                      hashcode = '{row[3]}'
                                                  WHERE id = {row[0]}
                                                "
                        );
                    }
                    return row;
                },
                allEntriesInDestination
            );

            VoidDestination<string[]> voidDest = new VoidDestination<string[]>();
            source.LinkTo(trans);
            trans.LinkTo(lookup);
            lookup.LinkTo(voidDest);

            source.Execute();
            voidDest.Wait();

            //Assert
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "dbo.HashMatchDestination",
                    $"id = 1 AND name='Bugs' AND age = 12 AND hashcode = '{HashHelper.Encrypt_Char40("1Bugs12")}'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "dbo.HashMatchDestination",
                    $"id = 2 AND name='Coyote' AND age = 8 AND hashcode = '{HashHelper.Encrypt_Char40("2Coyote8")}'"
                )
            );
        }
    }
}
