namespace ETLBox.Connection
{
    /// <summary>
    /// The generic defintion of a connection string
    /// </summary>
    public interface IDbConnectionString
    {
        string Value { get; set; }
        string ToString();
        IDbConnectionString Clone();
        string DbName { get; set; }
        //bool HasMasterDbName { get; }
        string MasterDbName { get; }
        IDbConnectionString CloneWithNewDbName(string value = null);
        IDbConnectionString CloneWithMasterDbName();
    }
}
