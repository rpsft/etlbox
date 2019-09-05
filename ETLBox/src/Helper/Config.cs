using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ETLBox.Helper
{
    public static class Config
    {
        public static string SqlRawConnectionString(string section)
            => Config.DefaultConfigFile.GetSection(section)["SqlConnectionString"];
        public static ConnectionString SqlConnectionString(string section)
            => new ConnectionString(SqlRawConnectionString(section));
        public static SqlConnectionManager SqlConnectionManager(string section)
            => new SqlConnectionManager(SqlConnectionString(section));

        static string SSASRawConnectionString(string section)
            => Config.DefaultConfigFile.GetSection(section)["SSASConnectionString"];
        public static ConnectionString SSASConnectionString(string section)
            => new ConnectionString(SSASRawConnectionString(section));
        public static AdomdConnectionManager SSASConnectionManager(string section)
            => new AdomdConnectionManager(SqlConnectionString(section));

        public static string SQLiteRawConnectionString(string section)
            => Config.DefaultConfigFile.GetSection(section)["SQLiteConnectionString"];
        public static SQLiteConnectionString SQLiteConnectionString(string section)
            => new SQLiteConnectionString(SQLiteRawConnectionString(section));
        public static SQLiteConnectionManager SQLiteConnectionManager(string section)
            => new SQLiteConnectionManager(SQLiteConnectionString(section));

        static IConfigurationRoot _defaultConfigFile;
        public static IConfigurationRoot DefaultConfigFile
        {
            get
            {
                if (_defaultConfigFile == null)
                    Load("default.config.json");
                return _defaultConfigFile;
            }
            set{
                _defaultConfigFile = value;
            }
        }

        public static void Load(string configFile)
        {
            DefaultConfigFile = new ConfigurationBuilder()
                    .AddJsonFile(configFile)
                    .Build();
        }

    }
}
