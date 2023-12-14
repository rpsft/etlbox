using System.IO;
using ALE.ETLBox.src.Definitions.Database;
using TestShared.src.Helper;

namespace ALE.ETLBoxTests.Performance.src.Helper
{
    public static class BigDataCsvSource
    {
        internal static List<TableColumn> DestTableCols { get; } =
            new()
            {
                new TableColumn("Col1", "CHAR(255)", allowNulls: false),
                new TableColumn("Col2", "CHAR(255)", allowNulls: false),
                new TableColumn("Col3", "CHAR(255)", allowNulls: false),
                new TableColumn("Col4", "CHAR(255)", allowNulls: true)
            };

        internal static string CSVFolderName = "res/Csv";

        internal static string GetCompleteFilePath(int numberOfRows) =>
            Path.GetFullPath(Path.Combine(CSVFolderName, "TestData" + numberOfRows + ".csv"));

        internal static void CreateCsvFileIfNeeded(int numberOfRows)
        {
            if (File.Exists(GetCompleteFilePath(numberOfRows)))
            {
                return;
            }

            var bigData = new BigDataHelper
            {
                FileName = GetCompleteFilePath(numberOfRows),
                NumberOfRows = numberOfRows,
                TableDefinition = new TableDefinition("CSV", DestTableCols)
            };
            bigData.CreateBigDataCSV();
        }
    }
}
