using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public class ErrorHandler
    {
        public ISourceBlock<ETLBoxError> ErrorSourceBlock => ErrorBuffer;
        internal BufferBlock<ETLBoxError> ErrorBuffer { get; set; }
        internal bool HasErrorBuffer => ErrorBuffer != null;

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target, Task completion)
        {
            ErrorBuffer = new BufferBlock<ETLBoxError>();
            ErrorSourceBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            //var tbt = TransformBlock.Completion;
            completion.ContinueWith(t => ErrorBuffer.Complete());
        }

        public void Post(Exception e, string jsonRow)
        {
            ErrorBuffer.Post(new ETLBoxError()
            {
                Exception = e,
                ErrorText = e.ToString(),
                ReportTime = DateTime.Now,
                RecordAsJson = jsonRow
            });
        }

        public string ConvertIntoJson<T>(T row)
        {
            return JsonConvert.SerializeObject(row, new JsonSerializerSettings()
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.Indented
            });
        }
    }
}
