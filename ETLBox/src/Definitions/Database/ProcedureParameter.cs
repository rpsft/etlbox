using System;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// A parameter used in a procedure
    /// </summary>
    public class ProcedureParameter
    {
        /// <summary>
        /// Name of the procedure parameter
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Sql data type of the procedure parameter
        /// </summary>
        public string DataType { get; set; }

        /// <summary>
        /// The default value of the parameter
        /// </summary>
        public string DefaultValue { get; set; }
        internal bool HasDefaultValue => !String.IsNullOrWhiteSpace(DefaultValue);

        /// <summary>
        /// Indicates that the parameter is read only
        /// </summary>
        public bool ReadOnly { get; set; }

        /// <summary>
        /// Indicates that the parameter is used as output
        /// </summary>
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
