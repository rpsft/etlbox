using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class AggregationTypeInfo
    {
        private Dictionary<string, PropertyInfo> OutputPropertiesByName { get; set; } = new Dictionary<string, PropertyInfo>();
        private List<Tuple<PropertyInfo, string>> GroupColumns { get; set; } = new List<Tuple<PropertyInfo, string>>();
        internal List<Tuple<PropertyInfo, PropertyInfo>> GroupColumnsInputAndOutput { get; set; } = new List<Tuple<PropertyInfo, PropertyInfo>>();
        private string AggregateColumnNameInOutput { get; set; }
        internal PropertyInfo AggregateColumnInInput { get; set; }
        internal PropertyInfo AggregateColumnInOutput { get; set; }
        internal AggregationMethod AggregationMethod { get; set; }

        internal bool IsArray { get; set; } = true;
        internal bool IsDynamic { get; set; }

        internal AggregationTypeInfo(Type inputType, Type aggType)
        {
            IsArray = inputType.IsArray || aggType.IsArray;
            IsDynamic = typeof(IDynamicMetaObjectProvider).IsAssignableFrom(inputType) || typeof(IDynamicMetaObjectProvider).IsAssignableFrom(aggType);

            if (!IsArray && !IsDynamic)
            {
                foreach (var propInfo in inputType.GetProperties())
                {
                    AddAggregateColumn(propInfo);
                    AddGroupColumn(propInfo);
                }

                foreach (var propInfo in aggType.GetProperties())
                    OutputPropertiesByName.Add(propInfo.Name, propInfo);

                CombineInputAndOutputTypeInfo();
            }

        }

        private void AddGroupColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(GroupColumn)) as GroupColumn;
            if (attr != null)
                GroupColumns.Add(Tuple.Create(propInfo, attr.AggregationGroupingProperty));
        }

        private void AddAggregateColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(AggregateColumn)) as AggregateColumn;
            if (attr != null)
            {
                AggregateColumnInInput = propInfo;
                AggregateColumnNameInOutput = attr.AggregationProperty;
                AggregationMethod = attr.AggregationMethod;
            }
        }

        private void CombineInputAndOutputTypeInfo()
        {
            foreach (var mcp in GroupColumns)
            {
                if (!OutputPropertiesByName.ContainsKey(mcp.Item2))
                    throw new ETLBoxException($"Match column {mcp.Item2} does not exists in lookup source object!");
                GroupColumnsInputAndOutput.Add(Tuple.Create(mcp.Item1, OutputPropertiesByName[mcp.Item2]));
            }

            if (AggregateColumnNameInOutput != null)
            {
                if (!OutputPropertiesByName.ContainsKey(AggregateColumnNameInOutput))
                    throw new ETLBoxException($"Aggregation column {AggregateColumnNameInOutput} does not exists in aggregation output object!");
                AggregateColumnInOutput = OutputPropertiesByName[AggregateColumnNameInOutput];
            }
        }
    }
}


