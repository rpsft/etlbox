using ETLBox.Connection;
using System;
using System.ComponentModel;
using System.Data;
using System.Text.RegularExpressions;

namespace ETLBox.Helper
{
    public interface IDataTypeConverter
    {
       string TryConvertDbDataType(string dbSpecificTypeName, ConnectionManagerType connectionType);
    }
}
