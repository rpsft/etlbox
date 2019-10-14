using System;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines which column index is mapped to the property.
    /// By default, when reading from an excel file, the sequence of the properties is used to store the data.
    /// With this attribute, you can define the column index of the excel column for the property.
    /// The index starts at 0.
    /// </summary>
    /// <example>
    ///  public class MyPoco
    /// {
    ///     [ExcelColumn(2)]
    ///     public string ThirdColumnInExcel { get; set; }
    /// }
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public class ExcelColumn : Attribute
    {
        public int Index { get; set; }
        public ExcelColumn(int columnIndex)
        {
            Index = columnIndex;
        }
    }
}
