using ETLBox;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using System.IO;

namespace ETLBoxTests.Performance
{
    public static class BigDataCsvSource
    {
        internal static List<TableColumn> DestTableCols { get; } = new List<TableColumn>() {
                new TableColumn("Col1", "CHAR(255)", allowNulls: false),
                new TableColumn("Col2", "CHAR(255)", allowNulls: false),
                new TableColumn("Col3", "CHAR(255)", allowNulls: false),
                new TableColumn("Col4", "CHAR(255)", allowNulls: true),
            };

        internal static string CSVFolderName = "res/Csv";
        internal static string GetCompleteFilePath(int numberOfRows) =>
            Path.GetFullPath(Path.Combine(CSVFolderName, "TestData" + numberOfRows + ".csv"));

        internal static void CreateCSVFileIfNeeded(int numberOfRows)
        {
            if (!File.Exists(GetCompleteFilePath(numberOfRows)))
            {
                BigDataHelper bigData = new BigDataHelper()
                {
                    FileName = GetCompleteFilePath(numberOfRows),
                    NumberOfRows = numberOfRows,
                    TableDefinition = new TableDefinition("CSV", DestTableCols)
                };
                bigData.CreateBigDataCSV();
            }
        }


    }
}
