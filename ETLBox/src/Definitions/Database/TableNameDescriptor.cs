using ALE.ETLBox.ConnectionManager;
using System;
using System.Text.RegularExpressions;

namespace ALE.ETLBox
{
    public class ObjectNameDescriptor
    {
        string Expr
        {
            get
            {
                string EQB = QB == "[" || QB == "" ? @"\[" : QB;
                string EQE = QE == "]" || QE == "" ? @"\]" : QB;
                return $@"\.?{EQB}.+?{EQE}|[^{EQB}]+?(?=\.)|[^{EQB}]+"; //\.?\[.+?\]|[^\[]+?(?=\.)|[^\[]+
                
            }
        }
        public string Schema { get; set; }
        public string Table { get; set; }
        //ObjectName.IndexOf('.') > 0 ?
        //bjectName.Substring(0, ObjectName.IndexOf('.')) : string.Empty;
        //public string Table => ObjectName.IndexOf('.') > 0
        //    ? ObjectName.Substring(ObjectName.LastIndexOf('.') + 1) : ObjectName;
        public string QuotatedObjectName => Table.StartsWith(QB) ? Table : QB + Table + QE;
        public string UnquotatedObjectName => Table.StartsWith(QB) ? Table.Replace(QB, string.Empty).Replace(QE, string.Empty) : Table;
        public string UnquotatedSchemaName =>
            String.IsNullOrWhiteSpace(Schema) ? string.Empty : Schema.StartsWith(QB) ?
            Schema.Replace(QB, string.Empty).Replace(QE, string.Empty) : Schema;
        public string QuotatedSchemaName =>
            String.IsNullOrWhiteSpace(Schema) ? string.Empty : Schema.StartsWith(QB) ? Schema : QB + Schema + QE;
        public string QuotatedFullName =>
            String.IsNullOrWhiteSpace(Schema) ? QuotatedObjectName : QuotatedSchemaName + '.' + QuotatedObjectName;
        public string UnquotatedFullName =>
           String.IsNullOrWhiteSpace(Schema) ? UnquotatedObjectName : UnquotatedSchemaName + '.' + UnquotatedObjectName;

        public string ObjectName { get; private set; }
        public ConnectionManagerType ConnectionType { get; private set; }

        public string QB => ConnectionManagerSpecifics.GetBeginQuotation(ConnectionType);
        public string QE => ConnectionManagerSpecifics.GetEndQuotation(ConnectionType);
        public ObjectNameDescriptor(string objectName, ConnectionManagerType connectionType)
        {
            this.ObjectName = objectName;
            this.ConnectionType = connectionType;
            ParseSchemaAndTable();
        }

        public ObjectNameDescriptor(string objectName, IConnectionManager connection)
        {
            this.ObjectName = objectName;
            this.ConnectionType = ConnectionManagerSpecifics.GetType(connection);
            ParseSchemaAndTable();
        }

        private void ParseSchemaAndTable()
        {
            var m = Regex.Matches(ObjectName, Expr, RegexOptions.IgnoreCase);
            if (m.Count == 0)
                throw new ETLBoxException($"Unable to retrieve object name (and possible schema) from {ObjectName}.");
            else if (m.Count > 2)
                throw new ETLBoxException($"Unable to retrieve table and schema name from {ObjectName} - found {m.Count} possible matches.");
            else if (m.Count == 1)
                Table = m[0].Value.Trim();
            else if (m.Count == 2)
            {
                Schema = m[0].Value.Trim();
                Table = m[1].Value.Trim().StartsWith(".") ? m[1].Value.Trim().Substring(1) : m[1].Value.Trim();
            }
        }
    }
}
