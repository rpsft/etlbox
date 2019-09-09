using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Restores a database from a backup.
    /// </summary>
    public class RestoreDatabaseTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "RESTOREDB";
        public override string TaskName => $"Restore DB {DatabaseName} from {Path.GetFullPath(FileName)}";


        public override void Execute() {
            DefaultDataPath = (string)new SqlTask(this, DefaultDataPathSql) { TaskName = $"Read default data path" }.ExecuteScalar();
            FileList = new List<BackupFile>();
            new SqlTask(this, FileListSql) {
                TaskName = $"Read file list in backup file {Path.GetFullPath(FileName)}",
                BeforeRowReadAction = () => CurrentBackupFile = new BackupFile(),
                AfterRowReadAction = () => FileList.Add(CurrentBackupFile),
                Actions = new List<Action<object>>() {
                    logicalName => CurrentBackupFile.LogicalName = (string)logicalName,
                    physicalName => CurrentBackupFile.PhysicalName = (string)physicalName,
                    type => CurrentBackupFile.Type = (string)type,
                    filegroupname => CurrentBackupFile.FileGroupName = (string)filegroupname,
                    size => { },
                    MaxSize => { },
                    fileid => CurrentBackupFile.FileID = (long)fileid,
                }
            }.ExecuteReader();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string DatabaseName { get; set; }
        public string FileName { get; set; }
        public string Sql {
            get {
                return
    $@"
use [master]
restore database [{DatabaseName}] from  disk = N'{Path.GetFullPath(FileName)}' with file=1,
" +
String.Join("," + Environment.NewLine, FileList.OrderBy(file => file.FileID)
.Select(file => $"move N'{file.LogicalName}' to N'{Path.Combine(DefaultDataPath, DatabaseName + file.Suffix)}'"))
+ $@"
, NOUNLOAD, REPLACE";

            }
        }

        /* Some constructors */
        public RestoreDatabaseTask() {
        }

        public RestoreDatabaseTask(string databaseName, string fileName) : this() {
            DatabaseName = databaseName;
            FileName = fileName;
        }


        /* Static methods for convenience */
        public static void Restore(string databaseName, string fileName) => new RestoreDatabaseTask(databaseName, fileName).Execute();

        /* Implementation & stuff */
        string DefaultDataPathSql => "select cast(serverproperty('InstanceDefaultDataPath') as nvarchar(1000)) as DefaultDataPath";
        string FileListSql => $@"use [master]
restore filelistonly from disk=N'{Path.GetFullPath(FileName)}'";
        List<BackupFile> FileList { get; set; }

        internal class BackupFile {
            internal string LogicalName { get; set; }
            internal string PhysicalName { get; set; }
            internal long FileID { get; set; }
            internal string FileGroupName { get; set; }
            internal string Type { get; set; }
            internal string Suffix {
                get {
                    if (Type == "D")
                        return FileID > 1 ? $"_{FileID}.ndf" : ".mdf";
                    else
                        return FileID > 1 ? $"_{FileID}.log" : ".log";
                }
            }
        }

        BackupFile CurrentBackupFile { get; set; }
        string DefaultDataPath { get; set; }

    }


}
