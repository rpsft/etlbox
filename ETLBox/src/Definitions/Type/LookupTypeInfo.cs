using System;
using System.Collections.Generic;
using System.Reflection;

namespace ETLBox.DataFlow
{
    internal class LookupTypeInfo : MappingTypeInfo
    {
        internal List<AttributeMappingInfo> MatchColumns { get; set; } = new List<AttributeMappingInfo>();
        internal List<AttributeMappingInfo> RetrieveColumns { get; set; } = new List<AttributeMappingInfo>();

        internal LookupTypeInfo(Type inputType, Type sourceType) : base(inputType, sourceType)
        {
        }

        protected override void AddAttributeInfoMapping(PropertyInfo propInfo)
        {
            AddRetrieveColumn(propInfo);
            AddMatchColumn(propInfo);
        }


        private void AddMatchColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(MatchColumn)) as MatchColumn;
            if (attr != null)
                MatchColumns.Add(new AttributeMappingInfo()
                {
                    PropInOutput = propInfo,
                    PropNameInInput = attr.LookupSourcePropertyName
                });
        }

        private void AddRetrieveColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(RetrieveColumn)) as RetrieveColumn;
            if (attr != null)
                RetrieveColumns.Add(new AttributeMappingInfo()
                {
                    PropInOutput = propInfo,
                    PropNameInInput = attr.LookupSourcePropertyName
                });
        }

        protected override void CombineInputAndOutputMapping()
        {
            this.AssignInputProperty(MatchColumns);
            this.AssignInputProperty(RetrieveColumns);
        }
    }
}
