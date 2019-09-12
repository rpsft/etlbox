using ALE.ETLBox.ConnectionManager;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Creates or updates a procedure.
    /// </summary>
    /// <example>
    /// <code>
    /// CRUDProcedureTask.CreateOrAlter("demo.proc1", "select 1 as test");
    /// </code>
    /// </example>
    public class CreateProcedureTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATEPROC";
        public override string TaskName => $"{CreateOrAlterSql} procedure {ProcedureName}";
        public override void Execute() {
            if (ConnectionType == ConnectionManagerType.SQLLite)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");

            IsExisting = new IfExistsTask(ProcedureName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ProcedureName { get; set; }
        public string ProcedureDefinition { get; set; }
        public IList<ProcedureParameter> ProcedureParameters { get; set; }
        public string Sql => $@"{CreateOrAlterSql} PROCEDURE {ProcedureName}
{ParameterDefinition}
AS
BEGIN
SET NOCOUNT ON

{ProcedureDefinition}

END
        ";

        public CreateProcedureTask() {

        }
        public CreateProcedureTask(string procedureName, string procedureDefinition) : this() {
            this.ProcedureName = procedureName;
            this.ProcedureDefinition = procedureDefinition;
        }

        public CreateProcedureTask(string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter) : this(procedureName, procedureDefinition) {
            this.ProcedureParameters = procedureParameter;
        }

        public CreateProcedureTask(ProcedureDefinition definition) : this() {
            this.ProcedureName = definition.Name;
            this.ProcedureDefinition = definition.Definition;
            this.ProcedureParameters = definition.Parameter;
        }

        public static void CreateOrAlter(string procedureName, string procedureDefinition) => new CreateProcedureTask(procedureName, procedureDefinition).Execute();
        public static void CreateOrAlter(string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter)
            => new CreateProcedureTask(procedureName, procedureDefinition, procedureParameter).Execute();
        public static void CreateOrAlter(ProcedureDefinition procedure)
            => new CreateProcedureTask(procedure).Execute();
        public static void CreateOrAlter(IConnectionManager connectionManager, string procedureName, string procedureDefinition)
            => new CreateProcedureTask(procedureName, procedureDefinition) { ConnectionManager = connectionManager }.Execute();
        public static void CreateOrAlter(IConnectionManager connectionManager, string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter)
            => new CreateProcedureTask(procedureName, procedureDefinition, procedureParameter) { ConnectionManager = connectionManager }.Execute();
        public static void CreateOrAlter(IConnectionManager connectionManager, ProcedureDefinition procedure)
            => new CreateProcedureTask(procedure) { ConnectionManager = connectionManager }.Execute();

        bool IsExisting { get; set; }
        string CreateOrAlterSql => IsExisting ? "ALTER" : "CREATE";
        string ParameterDefinition => ProcedureParameters?.Count > 0 ?
                String.Join("," + Environment.NewLine, ProcedureParameters.Select(par => par.Sql))
                : String.Empty;

    }
}
