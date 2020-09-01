using ETLBox.Connection;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Creates or updates a procedure.
    /// </summary>
    /// <example>
    /// <code>
    /// CRUDProcedureTask.CreateOrAlter("demo.proc1", "select 1 as test");
    /// </code>
    /// </example>
    public class CreateProcedureTask : ControlFlowTask
    {
        /// <inheritdoc/>
        public override string TaskName => $"{CreateOrAlterSql} procedure {ProcedureName}";

        /// <summary>
        /// Creates or updates the procedure on the database if the database does support procedures.
        /// </summary>
        public void Execute()
        {
            if (!DbConnectionManager.SupportProcedures)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            IsExisting = new IfProcedureExistsTask(ProcedureName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (IsExisting && ConnectionType == ConnectionManagerType.MySql)
                new DropProcedureTask(ProcedureName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Drop();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /// <summary>
        /// The name of the procedure
        /// </summary>
        public string ProcedureName { get; set; }

        /// <summary>
        /// The formatted procedure name
        /// </summary>
        public ObjectNameDescriptor PN => new ObjectNameDescriptor(ProcedureName, QB, QE);

        /// <summary>
        /// The sql code of the procedure
        /// </summary>
        public string ProcedureDefinition { get; set; }

        /// <summary>
        /// The parameters for the procedure
        /// </summary>
        public IList<ProcedureParameter> ProcedureParameters { get; set; }

        /// <summary>
        /// The sql code that is used to create/update the procedure.
        /// </summary>
        public string Sql => $@"{CreateOrAlterSql} PROCEDURE {PN.QuotatedFullName}{ParameterDefinition}{Language}
{AS}
{BEGIN}

{ProcedureDefinition}

{END}
        ";

        public CreateProcedureTask()
        {

        }
        public CreateProcedureTask(string procedureName, string procedureDefinition) : this()
        {
            this.ProcedureName = procedureName;
            this.ProcedureDefinition = procedureDefinition;
        }

        public CreateProcedureTask(string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter) : this(procedureName, procedureDefinition)
        {
            this.ProcedureParameters = procedureParameter;
        }

        public CreateProcedureTask(ProcedureDefinition definition) : this()
        {
            this.ProcedureName = definition.Name;
            this.ProcedureDefinition = definition.Definition;
            this.ProcedureParameters = definition.Parameter;
        }

        /// <summary>
        /// Creates or updates a procedure.
        /// </summary>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="procedureDefinition">The sql code of the procedure</param>
        public static void CreateOrAlter(string procedureName, string procedureDefinition) => new CreateProcedureTask(procedureName, procedureDefinition).Execute();

        /// <summary>
        /// Creates or updates a procedure.
        /// </summary>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="procedureDefinition">The sql code of the procedure</param>
        /// <param name="procedureParameter">A list of the parameters for the procedure</param>
        public static void CreateOrAlter(string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter)
            => new CreateProcedureTask(procedureName, procedureDefinition, procedureParameter).Execute();

        /// <summary>
        /// Creates or updates a procedure.
        /// </summary>
        /// <param name="procedure">The procedure definition object containing procedure name, code and potential parameters</param>
        public static void CreateOrAlter(ProcedureDefinition procedure)
            => new CreateProcedureTask(procedure).Execute();

        /// <summary>
        /// Creates or updates a procedure.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="procedureDefinition">The sql code of the procedure</param>
        public static void CreateOrAlter(IConnectionManager connectionManager, string procedureName, string procedureDefinition)
            => new CreateProcedureTask(procedureName, procedureDefinition) { ConnectionManager = connectionManager }.Execute();

        /// <summary>
        /// Creates or updates a procedure.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="procedureName">The name of the procedure</param>
        /// <param name="procedureDefinition">The sql code of the procedure</param>
        /// <param name="procedureParameter">A list of the parameters for the procedure</param>
        public static void CreateOrAlter(IConnectionManager connectionManager, string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter)
            => new CreateProcedureTask(procedureName, procedureDefinition, procedureParameter) { ConnectionManager = connectionManager }.Execute();

        /// <summary>
        /// Creates or updates a procedure.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="procedure">The procedure definition object containing procedure name, code and potential parameters</param>
        public static void CreateOrAlter(IConnectionManager connectionManager, ProcedureDefinition procedure)
            => new CreateProcedureTask(procedure) { ConnectionManager = connectionManager }.Execute();

        bool IsExisting { get; set; }
        string CreateOrAlterSql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.Postgres || ConnectionType == ConnectionManagerType.Oracle)
                    return "CREATE OR REPLACE";
                else if (ConnectionType == ConnectionManagerType.MySql)
                    return "CREATE";
                else
                    return IsExisting ? "ALTER" : "CREATE";
            }
        }
        string ParameterDefinition
        {
            get
            {
                string result = "";
                if (ConnectionType == ConnectionManagerType.Postgres || ConnectionType == ConnectionManagerType.MySql

                    )
                    result += "(";
                result += ProcedureParameters?.Count > 0 ?
                String.Join(",", ProcedureParameters.Select(par => ParameterSql(par)))
                : String.Empty;
                if (ConnectionType == ConnectionManagerType.Postgres || ConnectionType == ConnectionManagerType.MySql)
                    result += ")";
                return result;
            }
        }

        private string ParameterSql(ProcedureParameter par)
        {
            string sql = Environment.NewLine + "";
            if (ConnectionType == ConnectionManagerType.SqlServer)
                sql += "@";
            if (ConnectionType == ConnectionManagerType.MySql)
                sql += par.Out ? "OUT " : "IN ";
            sql += $@"{par.Name} {par.DataType}";
            if (par.HasDefaultValue && ConnectionType != ConnectionManagerType.MySql)
                sql += $" = {par.DefaultValue}";
            if (par.Out && ConnectionType != ConnectionManagerType.MySql)
                sql += " OUT";
            if (par.ReadOnly)
                sql += " READONLY";
            return sql;
        }

        string Language => this.ConnectionType == ConnectionManagerType.Postgres ?
            Environment.NewLine + "LANGUAGE SQL" : "";
        string BEGIN => this.ConnectionType == ConnectionManagerType.Postgres ? "$$" : "BEGIN";
        string END => this.ConnectionType == ConnectionManagerType.Postgres ? "$$" : "END";
        string AS => this.ConnectionType == ConnectionManagerType.MySql ? "" : "AS";
    }
}
