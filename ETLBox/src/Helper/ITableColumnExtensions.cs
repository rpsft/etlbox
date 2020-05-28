using System;
using System.Collections.Generic;
using System.Linq;

namespace ETLBox.Helper
{
    public static class ITableColumExtensions
    {
        public static string AsString(this ITableColumn column, string tblName = "", string prefix = "", string suffix = "") =>
          (tblName != "" ? tblName + "." : "") + prefix + column.Name + suffix;

        public static string AsString(this IEnumerable<ITableColumn> columns, string tblName = "", string prefix = "", string suffix = "") =>
            string.Join(", ", columns.Select(col => col.AsString(tblName, prefix, suffix)));

        public static string AsStringWithNewLine(this IEnumerable<ITableColumn> columns, string tblName = "", string prefix = "", string suffix = "") =>
            string.Join(Environment.NewLine + ",", columns.Select(col => col.AsString(tblName, prefix, suffix)));
    }
}
