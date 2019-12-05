using ALE.ETLBox.ConnectionManager;
using System;

namespace ALE.ETLBox
{
    public class ObjectNameDescriptor
    {
        public string Schema => ObjectName.IndexOf('.') > 0 ?
            ObjectName.Substring(0, ObjectName.IndexOf('.')) : string.Empty;
        public string Table => ObjectName.IndexOf('.') > 0
            ? ObjectName.Substring(ObjectName.LastIndexOf('.') + 1) : ObjectName;
        public string QuotatedObjectName => Table.Trim().StartsWith(QB) ? Table : QB + Table + QE;
        public string UnquotatedObjectName => Table.Trim().StartsWith(QB) ? Table.Replace(QB, string.Empty).Replace(QE, string.Empty) : Table;
        public string UnquotatedSchemaName =>
            String.IsNullOrWhiteSpace(Schema) ? string.Empty : Schema.Trim().StartsWith(QB) ?
            Schema.Replace(QB, string.Empty).Replace(QE, string.Empty) : Schema;
        public string QuotatedSchemaName =>
            String.IsNullOrWhiteSpace(Schema) ? string.Empty : Schema.Trim().StartsWith(QB) ? Schema : QB + Schema + QE;
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
        }

        public ObjectNameDescriptor(string tableName, IConnectionManager connection)
        {
            this.ObjectName = tableName;
            this.ConnectionType = ConnectionManagerSpecifics.GetType(connection);
        }


    }
}
