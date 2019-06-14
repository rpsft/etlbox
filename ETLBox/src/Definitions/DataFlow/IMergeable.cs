using System;

namespace ALE.ETLBox.DataFlow {
    public interface IMergable
    {
        DateTime ChangeDate { get; set; }
        char ChangeAction { get; set; }
        string UniqueId { get; }
    }
}
