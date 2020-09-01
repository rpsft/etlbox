namespace ETLBox.Connection
{
    /// <summary>
    /// The generic defintion of a connection string
    /// </summary>
    public interface IDbConnectionString
    {
        /// <summary>
        /// The connection string value, e.g. "Server=localhost;Database=etlbox;"
        /// </summary>
        string Value { get; set; }

        /// <summary>
        /// Returns the connection string <see cref="Value"/>
        /// </summary>
        /// <returns>The new connection string</returns>
        string ToString();

        /// <summary>
        /// Creates a copy of the current connection
        /// </summary>
        /// <returns></returns>
        IDbConnectionString Clone();

        /// <summary>
        /// The database name
        /// </summary>
        string DbName { get; set; }

        /// <summary>
        /// The name of the master database (if applicable)
        /// </summary>
        string MasterDbName { get; }

        /// <summary>
        /// Clone the current connection string with a new database name
        /// </summary>
        /// <param name="value">The new database name</param>
        /// <returns>The new connection string</returns>
        IDbConnectionString CloneWithNewDbName(string value = null);

        /// <summary>
        /// Clone the current connection string with the master database
        /// </summary>
        /// <returns>The new connection string</returns>
        IDbConnectionString CloneWithMasterDbName();
    }
}
