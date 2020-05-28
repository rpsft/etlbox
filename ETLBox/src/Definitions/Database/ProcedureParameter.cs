using System;

namespace ETLBox
{
    public class ProcedureParameter
    {
        public string Name { get; set; }
        public string DataType { get; set; }
        public string DefaultValue { get; set; }
        public bool HasDefaultValue => !String.IsNullOrWhiteSpace(DefaultValue);
        public bool ReadOnly { get; set; }
        public bool Out { get; set; }

        public ProcedureParameter()
        {
        }

        public ProcedureParameter(string name, string dataType) : this()
        {
            Name = name;
            DataType = dataType;
        }

        public ProcedureParameter(string name, string dataType, string defaultValue) : this(name, dataType)
        {
            DefaultValue = defaultValue;
        }

    }
}
