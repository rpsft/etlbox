using ALE.ETLBox;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace ALE.ETLBox.Helper
{
    public class BigDataHelper
    {
        public string FileName { get; set; }
        public TableDefinition TableDefinition { get; set; }
        public int NumberOfRows { get; set; }

        public void CreateBigDataCSV()
        {
            using (FileStream stream = File.Open(FileName, FileMode.Create))
            using (StreamWriter writer = new StreamWriter(stream))
            {
                string header = String.Join(",", TableDefinition.Columns.Select(col => col.Name));
                writer.WriteLine(header);
                for (int i = 0; i < NumberOfRows; i++)
                {
                    string line = String.Join(",", TableDefinition.Columns.Select(col =>
                    {
                        int length = DataTypeConverter.GetStringLengthFromCharString(col.DataType);
                        return HashHelper.RandomString(length);
                    }));
                    writer.WriteLine(line);
                }
            }
        }

        public static TimeSpan LogExecutionTime(string name, Action action)
        {
            Stopwatch watch = new Stopwatch();
            LogTask.Warn($"Starting: {name}");
            watch.Start();
            action.Invoke();
            watch.Stop();
            LogTask.Warn($"Stopping: {name} -- Time elapsed: {watch.Elapsed.TotalMinutes} minutes.");
            return watch.Elapsed;
        }
    }
}
