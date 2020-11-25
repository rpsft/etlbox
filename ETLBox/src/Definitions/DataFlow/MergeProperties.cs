using System.Collections.Generic;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// A list of properties that describe on which the DbMerge can perform which merge operation.
    /// </summary>
    public class MergeProperties
    {
        /// <summary>
        /// Property names that are used to check if the columns match (id values are equal).
        /// </summary>
        public ICollection<IdColumn> IdPropertyNames { get; set; } = new List<IdColumn>();

        /// <summary>
        /// Property names that should be use to compare if the values of a column are equal, so that
        /// the DbMerge can decide if the column needs to be updated.
        /// </summary>
        public ICollection<CompareColumn> ComparePropertyNames { get; set; } = new List<CompareColumn>();

        /// <summary>
        /// Property names that describe which columns are actually updated (if an update of the row is necessary).
        /// Can be left empty, then all non id columns will be updated. 
        /// </summary>
        public ICollection<UpdateColumn> UpdatePropertyNames { get; set; } = new List<UpdateColumn>();

        /// <summary>
        /// List of property names and a to-be value that tells the DbMerge if this row can be deleted.
        /// </summary>
        public ICollection<DeleteColumn> DeletionProperties { get; set; } = new List<DeleteColumn>();

        /// <summary>
        /// The property name where the ChangeAction is stored. Must by of type <see cref="ETLBox.DataFlow.ChangeAction"/>
        /// </summary>
        internal string ChangeActionPropertyName { get; set; } = DEFAULT_CHANGEACTION_PROPNAME;

        internal static string DEFAULT_CHANGEACTION_PROPNAME = "ChangeAction";

        /// <summary>
        /// The property name where the date of the change is stored. Must be of type DateTime.
        /// </summary>
        internal string ChangeDatePropertyName { get; set; } = DEFAULT_CHANGEDATE_PROPNAME;
        internal static string DEFAULT_CHANGEDATE_PROPNAME = "ChangeDate";
    }

}
