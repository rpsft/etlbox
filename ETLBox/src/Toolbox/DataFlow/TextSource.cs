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
    /// Reads data from a text file.
    /// Each line is read as a string and converted into an object by the <see cref="ParseLineAction"/>.
    /// A line is defined as a sequence of characters followed by a line feed("\n"), a carriage return ("\r"),
    /// or a carriage return immediately followed by a line feed("\r\n").
    /// </summary>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    public class TextSource<TOutput> : DataFlowStreamSource<TOutput>, ILoggableTask, IDataFlowSource<TOutput>
    {
        #region Public properties

        /// <inheritdoc />
        public override string TaskName => $"Read text data Uri: {CurrentRequestUri ?? ""}";

        /// <summary>
        /// Define how many rows in the file should be skipped before actually reading any data.
        /// By default the reader start at the first line (no rows are skipped)
        /// </summary>
        public int SkipRows { get; set; } = 0;

        /// <summary>
        /// This Action is called for every line in the source document.
        /// The input is the line as as string and it expect as output the line parsed
        /// into the desired output type of the component.
        /// </summary>
        public Action<string, TOutput> ParseLineAction { get; set; }

        #endregion

        #region Constructors

        public TextSource()
        {
            TypeInfo = new TypeInfo(typeof(TOutput)).GatherTypeInfo();
            ResourceType = ResourceType.File;
        }

        /// <param name="uri">The source of the file. This can be a filename or a web url.<see cref="Uri"/></param>
        /// <param name="parseLineAction"><see cref="ParseLineAction"/></param>
        public TextSource(string uri, Action<string, TOutput> parseLineAction) : this()
        {
            Uri = uri;
            ParseLineAction = parseLineAction;
        }

        #endregion

        #region Implementation

        TypeInfo TypeInfo { get; set; }

        protected override void InitReader()
        {
            VerifyProperties();
            StreamReader = new StreamReader(Uri, Encoding.UTF8, true);
            SkipFirstRows();
        }

        private void VerifyProperties()
        {
            if (ParseLineAction == null)
                throw new ETLBoxException("A TextSource must have a PropertyMatch function defined!");
        }

        private void SkipFirstRows()
        {
            for (int i = 0; i < SkipRows; i++)
                StreamReader.ReadLine();
        }

        protected override void ReadAllRecords()
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
            TOutput newObject = default;
            try
            {
                if (TypeInfo.IsArray)
                    newObject= (TOutput)Activator.CreateInstance(typeof(TOutput), new object[] { 1 });
                else
                    newObject = (TOutput)Activator.CreateInstance(typeof(TOutput));

                ParseLineAction(line, newObject);
                if (!Buffer.SendAsync(newObject).Result)
                    throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
            }
            catch (ETLBoxException) { throw; }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, line);
            }

        }

        protected override void CloseReader()
        { }

        #endregion
    }

    /// <inheritdoc />
    public class TextSource : TextSource<ExpandoObject>
    {
        public TextSource() : base() { }
        public TextSource(string fileName, Action<string,ExpandoObject> parseLineAction) : base(fileName, parseLineAction) { }
    }
}
