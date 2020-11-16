
namespace ETLBox.DataFlow
{
    /// <summary>
    /// The mode of operation a DbMerge may work in.
    /// Full means that source contains all data, NoDeletions that source contains all data but no deletions are executed,
    /// Delta means that source has only delta information and deletions are deferred from a particular property and
    /// OnlyUpdates means that only updates are applied to the destination.
    /// </summary>
    public enum MergeMode
    {
        Delta, //does inserts & updates, deletions only with flag
        Full, //does inserts, updates & deletions (deletions if record is missing)
        InsertsAndUpdatesOnly, //does inserts & updates, no deletions (like delta, but deletion flag ignored)
        UpdatesOnly //only updates, no inserts nor deletions
    }
}
