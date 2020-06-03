using System.Collections.Generic;

namespace ETLBox.DataFlow
{
    public class MergeProperties
    {
        public List<string> IdPropertyNames { get; set; } = new List<string>();
        public List<string> ComparePropertyNames { get; set; } = new List<string>();
        public Dictionary<string, object> DeletionProperties { get; set; } = new Dictionary<string, object>();
        internal string ChangeActionPropertyName { get; set; } = "ChangeAction";
        internal string ChangeDatePropertyName { get; set; } = "ChangeDate";
    }

}
