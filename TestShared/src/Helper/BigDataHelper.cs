﻿using System.Diagnostics;
using System.IO;
using System.Linq;
using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Helper;
using ALE.ETLBox.src.Toolbox.Logging;

namespace TestShared.src.Helper
{
    public class BigDataHelper
    {
        public string FileName { get; set; }
        public TableDefinition TableDefinition { get; set; }
        public int NumberOfRows { get; set; }

        public void CreateBigDataCSV()
        {
            using FileStream stream = File.Open(FileName, FileMode.Create);
            using var writer = new StreamWriter(stream);
            var header = string.Join(",", TableDefinition.Columns.Select(col => col.Name));
            writer.WriteLine(header);
            for (var i = 0; i < NumberOfRows; i++)
            {
                var line = string.Join(
                    ",",
                    TableDefinition.Columns.Select(col =>
                    {
                        var length = DataTypeConverter.GetStringLengthFromCharString(
                            col.DataType
                        );
                        return HashHelper.RandomString(length);
                    })
                );
                writer.WriteLine(line);
            }
        }

        public static TimeSpan LogExecutionTime(string name, Action action)
        {
            var watch = new Stopwatch();
            LogTask.Warn($"Starting: {name}");
            watch.Start();
            action.Invoke();
            watch.Stop();
            LogTask.Warn(
                $"Stopping: {name} -- Time elapsed: {watch.Elapsed.TotalSeconds} seconds."
            );
            return watch.Elapsed;
        }
    }
}
