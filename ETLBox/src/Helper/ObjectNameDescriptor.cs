using ETLBox.Exceptions;
using System;
using System.Text.RegularExpressions;

namespace ETLBox.Helper
{
    /// <summary>
    /// Applies database specific formatting to an object names.
    /// E.g. schema.ViewName would create [schema].[ViewName] for SqlServer and "schema"."ViewName" for Postgres
    /// </summary>
    public class ObjectNameDescriptor
    {
        private string _schema;
        private string _table;

        /// <summary>
        /// The name of the object that needs to have database spcific formatting applied
        /// </summary>
        public string ObjectName { get; }

        /// <summary>
        /// The quotation begin character that is used in the database.
        /// E.g. SqlServer uses: '[' and Postgres: '"'
        /// </summary>
        public string QB { get; }

        /// <summary>
        /// The quotation end character that is used in the database.
        /// E.g. SqlServer uses: ']' and Postgres: '"'
        /// </summary>
        public string QE { get; }

        /// <summary>
        /// The object name with quotes.
        /// E.g. schema.ViewName would create "ViewName"
        /// </summary>
        public string QuotatedObjectName => _table.StartsWith(QB) ? _table : QB + _table + QE;

        /// <summary>
        /// The object name without any quoting.
        /// E.g. "schema"."ViewName" would create ViewName
        /// </summary>
        public string UnquotatedObjectName => _table.StartsWith(QB) ? _table.Replace(QB, string.Empty).Replace(QE, string.Empty) : _table;

        /// <summary>
        /// The schema name without any quoting.
        /// E.g. "schema"."ViewName" would create schema
        /// </summary>
        public string UnquotatedSchemaName =>
            String.IsNullOrWhiteSpace(_schema) ? string.Empty : _schema.StartsWith(QB) ?
            _schema.Replace(QB, string.Empty).Replace(QE, string.Empty) : _schema;

        /// <summary>
        /// The schema name with quotes.
        /// E.g. "schema"."ViewName" would create "schema"
        /// </summary>
        public string QuotatedSchemaName =>
            String.IsNullOrWhiteSpace(_schema) ? string.Empty : _schema.StartsWith(QB) ? _schema : QB + _schema + QE;

        /// <summary>
        /// The whole name with quotes.
        /// E.g. schema.ViewName would create "schema"."ViewName"
        /// </summary>
        public string QuotatedFullName =>
            String.IsNullOrWhiteSpace(_schema) ? QuotatedObjectName : QuotatedSchemaName + '.' + QuotatedObjectName;

        /// <summary>
        /// The whole name without any  quotation
        /// E.g. "schema"."ViewName" would create schema.ViewName
        /// </summary>
        public string UnquotatedFullName =>
           String.IsNullOrWhiteSpace(_schema) ? UnquotatedObjectName : UnquotatedSchemaName + '.' + UnquotatedObjectName;

        /// <summary>
        /// Creates a new instance and already parses the values. Right after initialization you can access the values in the properties.
        /// </summary>
        /// <param name="objectName">The full object name (e.g. Schema.ViewName)</param>
        /// <param name="qb">The database specific quotation start (e.g. '[' for Sql Server)</param>
        /// <param name="qe">The database specific quotation start (e.g. ']' for Sql Server)</param>
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
                string EQB = QB == "[" ? @"\[" : (QB == "" ? @"""" : QB);
                string EQE = QE == "]" ? @"\]" : (QE == "" ? @"""" : QB);

                //see also: https://stackoverflow.com/questions/60747665/regex-expression-for-parsing-sql-server-schema-and-tablename?noredirect=1#comment107559387_60747665
                return $@"\.? *(?:{EQB}[^{EQE}]+{EQE}|\w+)"; //Original Regex:  \.? *(?:\[[^]]+\]|\w+)
            }
        }
    }
}
