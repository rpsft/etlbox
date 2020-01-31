using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class LookupTypeInfo
    {
        internal Dictionary<string, PropertyInfo> SourcePropertiesByName { get; set; } = new Dictionary<string, PropertyInfo>();
        internal List<Tuple<PropertyInfo, string>> MatchColumns { get; set; } = new List<Tuple<PropertyInfo, string>>();
        internal List<Tuple<PropertyInfo, string>> RetrieveColumns { get; set; } = new List<Tuple<PropertyInfo, string>>();
        internal List<Tuple<PropertyInfo, PropertyInfo>> MatchColumnsInputAndSource { get; set; } = new List<Tuple<PropertyInfo, PropertyInfo>>();
        internal List<Tuple<PropertyInfo, PropertyInfo>> RetrieveColumnsInputAndSource { get; set; } = new List<Tuple<PropertyInfo, PropertyInfo>>();

        internal bool IsArray { get; set; } = true;
        internal bool IsDynamic { get; set; }

        internal LookupTypeInfo(Type inputType,Type sourceType)
        {
            IsArray = inputType.IsArray || sourceType.IsArray;
            IsDynamic = typeof(IDynamicMetaObjectProvider).IsAssignableFrom(inputType) || typeof(IDynamicMetaObjectProvider).IsAssignableFrom(sourceType);

            if (!IsArray && !IsDynamic)
            {
                foreach (var propInfo in inputType.GetProperties())
                {
                    AddRetrieveColumn(propInfo);
                    AddMatchColumn(propInfo);
                }

                foreach (var propInfo in sourceType.GetProperties())
                    SourcePropertiesByName.Add(propInfo.Name, propInfo);

                CombineInputAndSourceTypeInfo();
            }

        }

        private void AddMatchColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(MatchColumn)) as MatchColumn;
            if (attr != null)
                MatchColumns.Add(Tuple.Create(propInfo, attr.LookupSourcePropertyName));
        }

        private void AddRetrieveColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(RetrieveColumn)) as RetrieveColumn;
            if (attr != null)
                RetrieveColumns.Add(Tuple.Create(propInfo, attr.LookupSourcePropertyName));
        }

        private void CombineInputAndSourceTypeInfo()
        {
            foreach (var mcp in MatchColumns) {
                if (!SourcePropertiesByName.ContainsKey(mcp.Item2))
                    throw new ETLBoxException($"Match column {mcp.Item2} does not exists in lookup source object!");
                MatchColumnsInputAndSource.Add(Tuple.Create(mcp.Item1, SourcePropertiesByName[mcp.Item2]));
            }

            foreach (var rcp in RetrieveColumns) {
                if (!SourcePropertiesByName.ContainsKey(rcp.Item2))
                    throw new ETLBoxException($"Match column {rcp.Item2} does not exists in lookup source object!");
                RetrieveColumnsInputAndSource.Add(Tuple.Create(rcp.Item1, SourcePropertiesByName[rcp.Item2]));
            }
        }
    }
}

