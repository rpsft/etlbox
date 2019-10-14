using System;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines a the Id column name used for the DBMerge
    /// If you do not provide this attribute on the UniqueId property (used by the IMergable interface),
    /// the default is that the table data is read into the memory and before rewriting the data back the
    /// table will be truncated.
    /// </summary>
    /// <example>
    ///  public class MyPoco : IMergable
    /// {
    ///     public int Key { get; set; }
    ///     public string Value {get;set; }
    ///     [MergeIdColumName("Key")]
    ///     public string UniqueId => Key;
    /// }
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public class MergeIdColumnName : Attribute
    {
        public string IdColumnName { get; set; }
        public MergeIdColumnName(string idColumnName)
        {
            IdColumnName = idColumnName;
        }
    }
}
