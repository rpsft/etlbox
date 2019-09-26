using ALE.ETLBox.ConnectionManager;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ETLBox
{
    public class TableNameDescriptor
    {
        public string Schema => FullName.IndexOf('.') > 0 ?
            FullName.Substring(0, FullName.IndexOf('.')) : string.Empty;
        public string Table => FullName.IndexOf('.') > 0
            ? FullName.Substring(FullName.LastIndexOf('.') + 1) : FullName;
        public string QuotatedTableName => Table.Trim().StartsWith(QB) ? Table : QB + Table + QE;
        public string QuotatedSchemaName =>
            String.IsNullOrWhiteSpace(Schema) ? string.Empty : Schema.Trim().StartsWith(QB) ? Schema : QB + Schema + QE;
        public string QuotatedFullName =>
            String.IsNullOrWhiteSpace(Schema) ?  QuotatedTableName : QuotatedSchemaName + '.' + QuotatedTableName;

        public string FullName { get; private set; }
        public ConnectionManagerType ConnectionType { get; private set; }

        public string QB => ConnectionManagerSpecifics.GetBeginQuotation(ConnectionType);
        public string QE => ConnectionManagerSpecifics.GetEndQuotation(ConnectionType);
        public TableNameDescriptor(string tableName, ConnectionManagerType connectionType)
        {
            this.FullName = tableName;
            this.ConnectionType = connectionType;
        }

        public TableNameDescriptor(string tableName, IConnectionManager connection)
        {
            this.FullName = tableName;
            this.ConnectionType = ConnectionManagerSpecifics.GetType(connection) ;
        }


    }
}
