using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ALE.ETLBox.DataFlow
{
    internal class AttributeMappingInfo
    {
        internal PropertyInfo PropInInput { get; set; }
        internal string PropNameInOutput { get; set; }
        internal PropertyInfo PropInOutput { get; set; }
    }
}
