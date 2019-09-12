using System;

namespace ALE.ETLBox.DataFlow {
    public interface IMergable
    {
        DateTime ChangeDate { get; set; }
        string ChangeAction { get; set; }
        string UniqueId { get; }
    }
}
