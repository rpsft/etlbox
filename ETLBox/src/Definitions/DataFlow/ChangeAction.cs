using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ETLBox.DataFlow
{
    public enum ChangeAction : ushort
    {
        Exists = 0,
        Insert = 1,
        Update = 2,
        Delete = 3
    }
}
