using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// Defines how columns are mapped to each other when renaming them. 
    /// For arrays it defines which index in the is renamed. CurrentName can be left empty then. 
    /// </summary>
    public class ColumnMapping
    {
        /// <summary>
        /// Index of the element in the array
        /// </summary>
        public int? ArrayIndex { get; set; }

        /// <summary>
        /// Current name of the column or property
        /// </summary>
        public string CurrentName { get; set; }

        /// <summary>
        /// New name of the column or property 
        /// </summary>
        public string NewName { get; set; }
        public ColumnMapping()
        {

        }

        public ColumnMapping(string currentName, string newName)
        {
            CurrentName = currentName;
            NewName = newName;
        }

        public ColumnMapping(int arrayIndex, string newName)
        {
            ArrayIndex = arrayIndex;
            NewName = newName;
        }
    }
}
