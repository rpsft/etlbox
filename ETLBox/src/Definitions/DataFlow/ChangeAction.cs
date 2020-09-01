namespace ETLBox.DataFlow
{
    /// <summary>
    /// When comparing two data sets regarding their changes, this enumeration describe how they are different-
    /// Exists: Both are equal, Insert: This record is inserted, Update: This record is updated, Delete: This record is deleted.
    /// </summary>
    public enum ChangeAction
    {
        Exists = 0,
        Insert = 1,
        Update = 2,
        Delete = 3
    }
}
