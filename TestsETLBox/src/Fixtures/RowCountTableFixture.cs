using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ETLBoxTests.Fixtures
{
    public class RowCountTableFixture
    {
        public RowCountTableFixture()
        {
            SqlTask.ExecuteNonQuery(Config.SqlConnectionManager("ControlFlow")
                , "Create test data table"
                , $@"
CREATE TABLE RowCountTest
(
    Col1 INT NULL
)
INSERT INTO RowCountTest
SELECT * FROM
(VALUES (1), (2), (3)) AS MyTable(v)");
        }
    }
}
