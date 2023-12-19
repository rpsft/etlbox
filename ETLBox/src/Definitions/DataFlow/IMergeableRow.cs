namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Represents table row in destination database for <see cref="DbMerge{T}"/>
    /// </summary>
    public interface IMergeableRow
    {
        /// <summary>
        /// Time when the object was considered for merging
        /// </summary>
        /// <remarks>
        /// When <see cref="ChangeAction"/> is set, this value is set to <see cref="DateTime.Now"/>
        /// </remarks>
        /// <value>null means not determined yet</value>
        DateTime ChangeDate { get; set; }

        /// <summary>
        /// The result of a merge operation
        /// </summary>
        /// <value>null means not determined yet</value>
        ChangeAction? ChangeAction { get; set; }
    }
}
