using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ALE.ETLBox {
    public class QueryParameter {
        public string Name { get; set; } 
        public string Type { get; set; }        
        public object Value { get; set; }

        public DbType DBType => DataTypeConverter.GetDBType(Type);
        
        public QueryParameter(string name, string type, object value) {
            Name = name;
            Type = type;
            Value = value;
        }
    }
}
