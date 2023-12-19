using System;
using System.Dynamic;
using ALE.ETLBox.Common.DataFlow;
using JetBrains.Annotations;

namespace TestHelper.Models
{
    [PublicAPI]
    public class BrokenTransformation : RowTransformation<ExpandoObject>
    {
        public BrokenTransformation()
        {
            TransformationFunc = source =>
            {
                try
                {
                    throw new InvalidDataException("test");
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer)
                        throw;
                    ErrorHandler.Send(e,ErrorHandler.ConvertErrorData(source));
                }

                return new ExpandoObject();
            };
        }
    }
}
