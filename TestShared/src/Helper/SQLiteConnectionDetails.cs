using System.IO;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;

namespace TestShared.Helper;

/// <summary>
/// Generates new database name for each connection.
/// </summary>
public class SQLiteConnectionDetails : ConnectionDetails<
    SQLiteConnectionString,
    SQLiteConnectionManager
>
{
    public SQLiteConnectionDetails(string connectionStringName) : base(connectionStringName)
    {
    }
    
    public SQLiteConnectionString ConnectionStringToTemplate(string section) =>
        new()
        {
            Value = RawConnectionString(section)
        };

    public new SQLiteConnectionString ConnectionString(string section, string dbNameSuffix = null)
    {
        var connectionString = new SQLiteConnectionString()
        {
            Value = RawConnectionString(section)
        };
        connectionString.DbName = SqliteFileName(section, dbNameSuffix);
        return connectionString;
    }
        
    public new SQLiteConnectionManager ConnectionManager(string section)
    {
        return new SQLiteConnectionManager
        {
            ConnectionString = ConnectionString(section)
        };
    }
    
    public static void CopyFromTemplate(string section, string dbNameSuffix)
    {
        var sqliteTemplateFilePath = Config.SQLiteConnection.ConnectionStringToTemplate(section).DbName;
        var sqliteFileName = SqliteFileName(section, dbNameSuffix);
        if (File.Exists(sqliteFileName))
        {
            return;
        }

        // Extract the directory part from the file path
        var directoryPath = Path.GetDirectoryName(sqliteFileName);

        // Check if the directory exists
        if (!Directory.Exists(directoryPath))
        {
            // Create the directory
            Directory.CreateDirectory(directoryPath!);
        }

        File.Copy(sqliteTemplateFilePath, sqliteFileName!);
    }
    
    public static void DeleteDatabase(string section, string dbNameSuffix)
    {
        var sqliteFileName = SqliteFileName(section, dbNameSuffix);
        if (!File.Exists(sqliteFileName))
        {
            return;
        }
        File.Delete(sqliteFileName);
    }

    private static string SqliteFileName(string section, string dbNameSuffix)
    {
        var baseName = Config.SQLiteConnection.ConnectionString(section).DbName;
        return dbNameSuffix != null
            ? Path.Join(baseName, $"{dbNameSuffix}.db")
            : Path.Join(baseName, "TestRun.db");
    }
}
