using ALE.ETLBox.Common;

namespace ALE.ETLBox.DataFlow
{
    internal abstract class MappingTypeInfo
    {
        private Dictionary<string, PropertyInfo> InputPropertiesByName { get; } = new();
        private bool IsArray => IsArrayInput || IsArrayOutput;
        private bool IsArrayInput { get; }
        internal bool IsArrayOutput { get; }
        private bool IsDynamic { get; }

        private protected MappingTypeInfo(Type inputType, Type outputType)
        {
            IsArrayInput = inputType.IsArray;
            IsArrayOutput = outputType.IsArray;
            IsDynamic =
                typeof(IDynamicMetaObjectProvider).IsAssignableFrom(inputType)
                || typeof(IDynamicMetaObjectProvider).IsAssignableFrom(outputType);
        }

        protected void InitMappings(Type inputType, Type outputType)
        {
            if (IsArray || IsDynamic)
            {
                return;
            }

            foreach (var propInfo in outputType.GetProperties())
                AddAttributeInfoMapping(propInfo);

            foreach (var propInfo in inputType.GetProperties())
                InputPropertiesByName.Add(propInfo.Name, propInfo);

            CombineInputAndOutputMapping();
        }

        protected abstract void AddAttributeInfoMapping(PropertyInfo propInfo);

        protected abstract void CombineInputAndOutputMapping();

        protected void AssignInputProperty(List<AttributeMappingInfo> columnList)
        {
            foreach (var attributeMappingInfo in columnList)
            {
                if (!InputPropertiesByName.TryGetValue(attributeMappingInfo.PropNameInInput, out PropertyInfo value))
                    throw new ETLBoxException(
                        $"Property {attributeMappingInfo.PropNameInInput} does not exists in target object!"
                    );
                attributeMappingInfo.PropInInput = value;
            }
        }
    }
}
