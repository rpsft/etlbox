using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal abstract class MappingTypeInfo
    {
        protected Dictionary<string, PropertyInfo> OutputPropertiesByName { get; set; } = new Dictionary<string, PropertyInfo>();
        internal bool IsArray { get; set; }
        internal bool IsArrayOutput { get; set; }
        internal bool IsDynamic { get; set; }

        internal MappingTypeInfo(Type inputType, Type outputType)
        {
            IsArrayOutput = outputType.IsArray;
            IsArray = inputType.IsArray || outputType.IsArray;
            IsDynamic = typeof(IDynamicMetaObjectProvider).IsAssignableFrom(inputType)
                || typeof(IDynamicMetaObjectProvider).IsAssignableFrom(outputType);

            if (!IsArray && !IsDynamic)
            {
                foreach (var propInfo in inputType.GetProperties())
                    AddAttributeInfoMapping(propInfo);

                foreach (var propInfo in outputType.GetProperties())
                    OutputPropertiesByName.Add(propInfo.Name, propInfo);

                CombineInputAndOutputMapping();
            }
        }

        protected abstract void AddAttributeInfoMapping(PropertyInfo propInfo);

        protected abstract void CombineInputAndOutputMapping();

        protected void AssignOutputProperty(List<AttributeMappingInfo> columnList)
        {
            foreach (var ami in columnList)
            {
                if (!OutputPropertiesByName.ContainsKey(ami.PropNameInOutput))
                    throw new ETLBoxException($"Property {ami.PropNameInOutput} does not exists in target object!");
                ami.PropInOutput = OutputPropertiesByName[ami.PropNameInOutput];
            }
        }

    }
}


