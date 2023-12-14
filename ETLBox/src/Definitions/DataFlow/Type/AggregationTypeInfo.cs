using System.Linq;
using ALE.ETLBox.src.Toolbox.DataFlow;

namespace ALE.ETLBox.src.Definitions.DataFlow.Type
{
    internal sealed class AggregationTypeInfo : MappingTypeInfo
    {
        internal List<AggregateAttributeMapping> AggregateColumns { get; } = new();
        internal List<AttributeMappingInfo> GroupColumns { get; } = new();

        internal AggregationTypeInfo(System.Type inputType, System.Type aggType)
            : base(inputType, aggType)
        {
            InitMappings(inputType, aggType);
        }

        protected override void AddAttributeInfoMapping(PropertyInfo propInfo)
        {
            AddAggregateColumn(propInfo);
            AddGroupColumn(propInfo);
        }

        private void AddGroupColumn(PropertyInfo propInfo)
        {
            if (
                propInfo.GetCustomAttribute(typeof(GroupColumnAttribute))
                is GroupColumnAttribute attr
            )
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
            if (
                propInfo.GetCustomAttribute(typeof(AggregateColumnAttribute))
                is AggregateColumnAttribute attr
            )
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

    internal sealed class AggregateAttributeMapping : AttributeMappingInfo
    {
        internal AggregationMethod AggregationMethod { get; set; }
    }
}
