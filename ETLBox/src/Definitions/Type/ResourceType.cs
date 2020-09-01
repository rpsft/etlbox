namespace ETLBox.DataFlow
{
    /// <summary>
    /// The type of resource you are using for a streaming source or destination.
    /// E.g. you can read Json data from a file or via http from a web service.
    /// </summary>
    public enum ResourceType
    {
        Unspecified = 0,
        Http = 1,
        File = 2
    }
}
