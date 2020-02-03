using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class AggregationTypeInfo : MappingTypeInfo
    {
        internal List<AggregateAttributeMapping> AggregateColumns { get; set; } = new List<AggregateAttributeMapping>();
        internal List<AttributeMappingInfo> GroupColumns { get; set; } = new List<AttributeMappingInfo>();

        internal AggregationTypeInfo(Type inputType, Type aggType) : base(inputType, aggType)
        {
        }

        protected override void AddAttributeInfoMapping(PropertyInfo propInfo)
        {
            AddAggregateColumn(propInfo);
            AddGroupColumn(propInfo);
        }

        private void AddGroupColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(GroupColumn)) as GroupColumn;
            if (attr != null)
            {
                GroupColumns.Add(new AttributeMappingInfo()
                {
                    PropInInput = propInfo,
                    PropNameInOutput = attr.AggregationGroupingProperty
                });
            }
        }

        private void AddAggregateColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(AggregateColumn)) as AggregateColumn;
            if (attr != null)
            {
                AggregateColumns.Add(new AggregateAttributeMapping()
                {
                    PropInInput = propInfo,
                    PropNameInOutput = attr.AggregationProperty,
                    AggregationMethod = attr.AggregationMethod
                });
            }
        }

        protected override void CombineInputAndOutputMapping()
        {
            this.AssignOutputProperty(GroupColumns);
            this.AssignOutputProperty(AggregateColumns.Cast<AttributeMappingInfo>().ToList());
        }
    }

    internal class AggregateAttributeMapping : AttributeMappingInfo
    {
        internal AggregationMethod AggregationMethod { get; set; }
    }
}


