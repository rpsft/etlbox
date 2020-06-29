using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using Microsoft.Extensions.Primitives;
using System;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using TheBoxOffice.LicenseManager;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Reads data from a text file. Each line is read as a string and written into the corresponding property
    /// identified by the ValueSelector.
    /// A line is defined as a sequence of characters followed by a line feed("\n"), a carriage return ("\r"), or a carriage return immediately followed by a line feed("\r\n").
    /// </summary>
    public class TextSource<TOutput> : DataFlowStreamSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read text data Uri: {CurrentRequestUri ?? ""}";

        /* Public properties */
        public int SkipRows { get; set; } = 0;
        public Action<string, TOutput> WriteLineIntoObject { get; set; }

        /* Private stuff */
        TypeInfo TypeInfo { get; set; }

        public TextSource()
        {
            TypeInfo = new TypeInfo(typeof(TOutput)).GatherTypeInfo();
            ResourceType = ResourceType.File;
        }

        public TextSource(string uri, Action<string,TOutput> writeLineIntoObject) : this()
        {
            Uri = uri;
            WriteLineIntoObject = writeLineIntoObject;
        }

        protected override void InitReader()
        {
            VerifyProperties();
            StreamReader = new StreamReader(Uri, Encoding.UTF8, true);
            SkipFirstRows();
        }

        private void VerifyProperties()
        {
            if (WriteLineIntoObject == null)
                throw new ETLBoxException("A TextSource must have a PropertyMatch function defined!");
        }

        private void SkipFirstRows()
        {
            for (int i = 0; i < SkipRows; i++)
                StreamReader.ReadLine();
        }

        protected override void ReadAll()
        {
            while (StreamReader.Peek() > 0)
            {
                ReadLineAndSendIntoBuffer();
                LogProgress();
            }
        }

        private void ReadLineAndSendIntoBuffer()
        {
            string line = StreamReader.ReadLine();
            try
            {
                TOutput newObject = default(TOutput);
                if (TypeInfo.IsArray)
                    newObject= (TOutput)Activator.CreateInstance(typeof(TOutput), new object[] { 1 });
                else
                    newObject = (TOutput)Activator.CreateInstance(typeof(TOutput));

                WriteLineIntoObject(line, newObject);
                Buffer.SendAsync(newObject).Wait();
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer) throw e;
                ErrorHandler.Send(e, line);
            }
        }

        protected override void CloseReader()
        { }
    }

    /// <summary>
    /// Reads data from a text file. Each line is read as a string and written into the corresponding property
    /// identified by the ValueSelector. Works internall with an ExpandoObject.
    /// A line is defined as a sequence of characters followed by a line feed("\n"), a carriage return ("\r"), or a carriage return immediately followed by a line feed("\r\n").
    /// </summary>
    public class TextSource : TextSource<ExpandoObject>
    {
        public TextSource() : base() { }
        public TextSource(string fileName, Action<string,ExpandoObject> writeLineIntoObject) : base(fileName, writeLineIntoObject) { }
    }
}
