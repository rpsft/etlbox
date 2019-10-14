namespace ALE.ETLBox
{
    /// <summary>
    /// The generic defintion of a connection string
    /// </summary>
    public interface IDbConnectionString
    {
        string Value { get; set; }
        string ToString();
    }
}
