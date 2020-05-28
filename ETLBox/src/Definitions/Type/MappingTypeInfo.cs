using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;

namespace ETLBox.DataFlow
{
    internal abstract class MappingTypeInfo
    {
        protected Dictionary<string, PropertyInfo> InputPropertiesByName { get; set; } = new Dictionary<string, PropertyInfo>();
        internal bool IsArray => IsArrayInput || IsArrayOutput;
        internal bool IsArrayInput { get; set; }
        internal bool IsArrayOutput { get; set; }
        internal bool IsDynamic { get; set; }

        internal MappingTypeInfo(Type inputType, Type outputType)
        {
            IsArrayInput = inputType.IsArray;
            IsArrayOutput = outputType.IsArray;
            IsDynamic = typeof(IDynamicMetaObjectProvider).IsAssignableFrom(inputType)
                || typeof(IDynamicMetaObjectProvider).IsAssignableFrom(outputType);

            if (!IsArray && !IsDynamic)
            {
                foreach (var propInfo in outputType.GetProperties())
                    AddAttributeInfoMapping(propInfo);

                foreach (var propInfo in inputType.GetProperties())
                    InputPropertiesByName.Add(propInfo.Name, propInfo);

                CombineInputAndOutputMapping();
            }
        }

        protected abstract void AddAttributeInfoMapping(PropertyInfo propInfo);

        protected abstract void CombineInputAndOutputMapping();

        protected void AssignInputProperty(List<AttributeMappingInfo> columnList)
        {
            foreach (var ami in columnList)
            {
                if (!InputPropertiesByName.ContainsKey(ami.PropNameInInput))
                    throw new ETLBoxException($"Property {ami.PropNameInInput} does not exists in target object!");
                ami.PropInInput = InputPropertiesByName[ami.PropNameInInput];
            }
        }

    }
}


