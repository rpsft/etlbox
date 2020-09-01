using System.Collections.Generic;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// Defines a procedure
    /// </summary>
    public class ProcedureDefinition
    {
        /// <summary>
        /// Name of the procedure
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The sql code of the procudure
        /// </summary>
        public string Definition { get; set; }

        /// <summary>
        /// List of parameters for the procedure
        /// </summary>
        public List<ProcedureParameter> Parameter { get; set; }

        public ProcedureDefinition()
        {
            Parameter = new List<ProcedureParameter>();
        }

        public ProcedureDefinition(string name, string definition) : this()
        {
            Name = name;
            Definition = definition;
        }

        public ProcedureDefinition(string name, string definition, List<ProcedureParameter> parameter) : this(name, definition)
        {
            Parameter = parameter;
        }


    }
}
