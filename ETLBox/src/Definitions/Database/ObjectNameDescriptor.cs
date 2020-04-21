using System;
using System.Text.RegularExpressions;

namespace ALE.ETLBox
{
    public class ObjectNameDescriptor
    {
        private string _schema;
        private string _table;
        
        public string ObjectName { get; }
        public string QB { get; }
        public string QE { get; }
        
        public string QuotatedObjectName => _table.StartsWith(QB) ? _table : QB + _table + QE;
        public string UnquotatedObjectName => _table.StartsWith(QB) ? _table.Replace(QB, string.Empty).Replace(QE, string.Empty) : _table;
        
        public string UnquotatedSchemaName =>
            String.IsNullOrWhiteSpace(_schema) ? string.Empty : _schema.StartsWith(QB) ?
            _schema.Replace(QB, string.Empty).Replace(QE, string.Empty) : _schema;
        public string QuotatedSchemaName =>
            String.IsNullOrWhiteSpace(_schema) ? string.Empty : _schema.StartsWith(QB) ? _schema : QB + _schema + QE;
        
        public string QuotatedFullName =>
            String.IsNullOrWhiteSpace(_schema) ? QuotatedObjectName : QuotatedSchemaName + '.' + QuotatedObjectName;
        public string UnquotatedFullName =>
           String.IsNullOrWhiteSpace(_schema) ? UnquotatedObjectName : UnquotatedSchemaName + '.' + UnquotatedObjectName;

        public ObjectNameDescriptor(string objectName, string qb, string qe)
        {
            ObjectName = objectName;
            QB = qb;
            QE = qe;
            
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
                _table = m[0].Value.Trim();
            else if (m.Count == 2)
            {
                _schema = m[0].Value.Trim();
                _table = m[1].Value.Trim().StartsWith(".") ? m[1].Value.Trim().Substring(1) : m[1].Value.Trim();
            }
        }
        
        private string Expr
        {
            get
            {
                string EQB = QB == "[" ? @"\[" : ( QB == "" ? @"""" : QB ) ;
                string EQE = QE == "]" ? @"\]" : ( QE == "" ? @"""" : QB ) ;
                
                //see also: https://stackoverflow.com/questions/60747665/regex-expression-for-parsing-sql-server-schema-and-tablename?noredirect=1#comment107559387_60747665
                return $@"\.? *(?:{EQB}[^{EQE}]+{EQE}|\w+)"; //Original Regex:  \.? *(?:\[[^]]+\]|\w+)
            }
        }
    }
}
