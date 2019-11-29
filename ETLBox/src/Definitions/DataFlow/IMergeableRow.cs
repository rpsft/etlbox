using System;

namespace ALE.ETLBox.DataFlow
{
    public interface IMergeableRow
    {
        DateTime ChangeDate { get; set; }
        string ChangeAction { get; set; }
        string UniqueId { get; }
    }
}
