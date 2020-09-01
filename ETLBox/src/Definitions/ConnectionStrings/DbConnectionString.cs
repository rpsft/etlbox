using System.Data.Common;

namespace ETLBox.Connection
{
    /// <summary>
    /// <see cref="IDbConnectionString"/> base
    /// </summary>
    /// <typeparam name="T">Derived type</typeparam>
    /// <typeparam name="TBuilder">The underlying ADO.NET connection string builder</typeparam>
    public abstract class DbConnectionString<T, TBuilder> : IDbConnectionString
        where T : DbConnectionString<T, TBuilder>, new()
        where TBuilder : DbConnectionStringBuilder, new()
    {
        public DbConnectionString()
        { }

        protected DbConnectionString(string value)
        {
            Value = value;
        }

        /// <summary>
        /// The underlying ADO.NET ConnectionStringBuilder
        /// </summary>
        public TBuilder Builder { get; private set; } = new TBuilder();

        /// <inheritdoc/>
        public virtual string Value
        {
            get => Builder.ConnectionString;
            set => Builder.ConnectionString = value;
        }

        /// <inheritdoc />
        public override string ToString() => Builder.ConnectionString;

        /// <inheritdoc />
        public abstract string DbName { get; set; }

        /// <summary>
        /// The keyword used in the connection string to identify a database
        /// </summary>
        protected abstract string DbNameKeyword { get; }

        /// <inheritdoc />
        public abstract string MasterDbName { get; }

        /// <inheritdoc
        IDbConnectionString IDbConnectionString.Clone() => Clone();

        /// <inheritdoc/>
        IDbConnectionString IDbConnectionString.CloneWithNewDbName(string value) => CloneWithNewDbName(value);

        /// <inheritdoc/>
        IDbConnectionString IDbConnectionString.CloneWithMasterDbName() => CloneWithMasterDbName();

        /// <summary>
        /// Clone the current connection string with a new database name
        /// </summary>
        /// <param name="value">The new database name</param>
        /// <returns>The new connection string</returns>
        public T CloneWithNewDbName(string value)
        {
            var clone = Clone();
            if (string.IsNullOrWhiteSpace(value))
            {
                clone.Builder.Remove(DbNameKeyword);
            }
            else
                clone.DbName = value;

            return clone;
        }

        /// <summary>
        /// Clones the current connection string
        /// </summary>
        /// <returns>A copy of the current connection string</returns>
        public virtual T Clone()
        {
            var clone = (T)MemberwiseClone();
            clone.Builder = new TBuilder();
            clone.Value = Value;
            return clone;
        }

        /// <summary>
        /// Clones the current connection string with removing the database name
        /// </summary>
        /// <returns>The new connection string without database name</returns>
        public T CloneWithoutDbName() => CloneWithNewDbName(string.Empty);

        /// <summary>
        /// Clones the current connection string with the master database name (if applicable)
        /// </summary>
        /// <returns>The new connection string with master database name</returns>
        public T CloneWithMasterDbName() => CloneWithNewDbName(MasterDbName);

    }
}
