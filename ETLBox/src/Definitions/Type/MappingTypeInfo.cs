using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;

namespace ETLBox.DataFlow
{
    internal abstract class MappingTypeInfo
    {
        internal Dictionary<string, PropertyInfo> InputPropertiesByName { get; set; } = new Dictionary<string, PropertyInfo>();
        internal Dictionary<string, PropertyInfo> OutputPropertiesByName { get; set; } = new Dictionary<string, PropertyInfo>();
        internal bool IsArray => IsArrayInput || IsArrayOutput;
        internal bool IsArrayInput { get; set; }
        internal bool IsArrayOutput { get; set; }
        internal bool IsDynamic => IsInputDynamic || IsOutputDynamic;
        internal bool IsInputDynamic { get; set; }
        internal bool IsOutputDynamic { get; set; }

        internal MappingTypeInfo(Type inputType, Type outputType)
        {
            IsArrayInput = inputType.IsArray;
            IsArrayOutput = outputType.IsArray;
            IsInputDynamic = typeof(IDynamicMetaObjectProvider).IsAssignableFrom(inputType);
            IsOutputDynamic = typeof(IDynamicMetaObjectProvider).IsAssignableFrom(outputType);

            if (!IsArray)
            {
                if (!IsOutputDynamic)
                {
                    foreach (var propInfo in outputType.GetProperties())
                    {
                        OutputPropertiesByName.Add(propInfo.Name, propInfo);
                        AddAttributeInfoMapping(propInfo);
                    }
                }

                if (!IsInputDynamic)
                {
                    foreach (var propInfo in inputType.GetProperties())
                    {
                        InputPropertiesByName.Add(propInfo.Name, propInfo);
                    }
                }

                if (!IsDynamic)
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


