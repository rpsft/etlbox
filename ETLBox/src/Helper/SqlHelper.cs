using System;

namespace ALE.ETLBox.Helper
{
    public class SqlHelper
    {
        public static string Headline1(string headline)
        {
            return string.Format(@"
------------------------------------------------------------
-- {0}
------------------------------------------------------------
"
                , headline.Replace(Environment.NewLine, Environment.NewLine + "--"));
        }

        public static string Headline2(string headline)
        {
            return string.Format(@"
/***
{0}
***/
", headline);
        }

        public static string Comment(string comment)
        {
            return string.Format(@"-- {0}", comment);
        }



        public static string DropTablesIfExists(params string[] tableNames)
        {
            string sql = "";
            foreach (string name in tableNames)
            {
                string objectName = name.StartsWith("#") ? "tempdb.." + name : name;
                sql += $@"if object_id('{objectName}') is not null drop table {name}" + Environment.NewLine;
            }
            return sql;
        }



        public static string Sequence(string sequenceName, string schema, int increment, string tableName, string keyName)
        {
            string sequence = "";
            sequence = $@"declare @MaxID{sequenceName} bigint
declare @sql{sequenceName} nvarchar(4000) 
if object_id('{schema}.{sequenceName}') is not null drop sequence {schema}.{sequenceName}
select @MaxID{sequenceName} = isnull(max({keyName}),0) from {tableName}
set @sql{sequenceName} = N'create sequence {schema}.{sequenceName}
	start with ' + cast((@MaxID{sequenceName}+{increment}) as nvarchar(50)) +'
	increment by {increment};'
execute sp_executesql @sql{sequenceName}
";

            return sequence;
        }




    }
}
