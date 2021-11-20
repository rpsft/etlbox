﻿using ALE.ETLBox.Helper;
using Microsoft.Data.SqlClient;
using System;

namespace ALE.ETLBox
{
    /// <summary>
    /// A helper class for encapsulating a connection string to a sql server in an object.
    /// Internally the SqlConnectionStringBuilder is used to access the values of the given connection string.
    /// </summary>
    public class SqlConnectionString :
        DbConnectionString<SqlConnectionString, SqlConnectionStringBuilder>
    {
        public SqlConnectionString() :
            base()
        { }

        public SqlConnectionString(string value) :
            base(value)
        { }

        public override string Value
        {
            get => base.Value.ReplaceIgnoreCase("Integrated Security=true", "Integrated Security=SSPI");
            set => base.Value = value;
        }

        public override string DbName
        {
            get => Builder.InitialCatalog;
            set => Builder.InitialCatalog = value;
        }
        public override string MasterDbName => "master";
        protected override string DbNameKeyword => "Database";

        public static implicit operator SqlConnectionString(string value) => new SqlConnectionString(value);
    }
}
