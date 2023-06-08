using System.Linq;

namespace ALE.ETLBox.DataFlow
{
    internal class AggregationTypeInfo : MappingTypeInfo
    {
        internal List<AggregateAttributeMapping> AggregateColumns { get; set; } = new();
        internal List<AttributeMappingInfo> GroupColumns { get; set; } = new();

        internal AggregationTypeInfo(Type inputType, Type aggType)
            : base(inputType, aggType) { }

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
                GroupColumns.Add(
                    new AttributeMappingInfo
                    {
                        PropInOutput = propInfo,
                        PropNameInInput = attr.AggregationGroupingProperty
                    }
                );
            }
        }

        private void AddAggregateColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(AggregateColumn)) as AggregateColumn;
            if (attr != null)
            {
                AggregateColumns.Add(
                    new AggregateAttributeMapping
                    {
                        PropInOutput = propInfo,
                        PropNameInInput = attr.AggregationProperty,
                        AggregationMethod = attr.AggregationMethod
                    }
                );
            }
        }

        protected override void CombineInputAndOutputMapping()
        {
            AssignInputProperty(GroupColumns);
            AssignInputProperty(AggregateColumns.Cast<AttributeMappingInfo>().ToList());
        }
    }

    internal class AggregateAttributeMapping : AttributeMappingInfo
    {
        internal AggregationMethod AggregationMethod { get; set; }
    }
}
