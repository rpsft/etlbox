using ETLBox.ControlFlow;
using System;
using System.Dynamic;
using System.Globalization;
using TheBoxOffice.LicenseManager;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Writes data into a text file. Each line in the output is created by calling the
    /// <see cref="WriteLineFunc"/> or by invoking ToString() on the object.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    public class TextDestination<TInput> : DataFlowStreamDestination<TInput>, ILoggableTask, IDataFlowDestination<TInput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName => $"Write text data into file {Uri ?? ""}";

        /// <summary>
        /// Defines how each row from the input is written into the file.
        /// The input for the Func is an object of the ingoing data type and return a string that is written into the target.
        /// </summary>
        public Func<TInput, string> WriteLineFunc { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// The default <see cref="ResourceType"/> for a TextDestination is a file.
        /// </summary>
        public TextDestination()
        {
            ResourceType = ResourceType.File;
        }

        /// <param name="filename">Will set the <see cref="Uri"/> to the given file name.</param>
        public TextDestination(string filename) : this()
        {
            Uri = filename;
        }

        /// <param name="filename">Will set the <see cref="Uri"/> to the given file name.</param>
        /// <param name="writeLineFunc">Sets the <see cref="WriteLineFunc"/></param>
        public TextDestination(string filename, Func<TInput, string> writeLineFunc) : this(filename)
        {
            WriteLineFunc = writeLineFunc;
        }

        #endregion

        #region Implementation

        protected override void InitStream()
        {
        }

        protected override void WriteIntoStream(TInput data)
        {
            WriteObject(data);
            LogProgress();
        }

        private void WriteObject(TInput data)
        {
            if (data == null) return;
            try
            {
                string line;
                if (WriteLineFunc != null)
                    line = WriteLineFunc?.Invoke(data);
                else
                    line = data.ToString();
                StreamWriter.WriteLine(line);
            }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, ErrorSource.ConvertErrorData(data));
            }
        }

        protected override void CloseStream()
        {
        }

        #endregion

    }

    /// <inheritdoc/>
    public class TextDestination : TextDestination<ExpandoObject>
    {
        public TextDestination() : base() { }

        public TextDestination(string fileName) : base(fileName) { }

        public TextDestination(string fileName, Func<ExpandoObject, string> writeLineFunc) : base(fileName, writeLineFunc) { }

    }

}
