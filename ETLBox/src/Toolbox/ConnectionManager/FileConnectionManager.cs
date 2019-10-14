using System;
using System.IO;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager to a file
    /// </summary>    
    public class FileConnectionManager
    {
        internal string FileName { get; set; }

        internal string Path { get; set; }

        internal string FullFileName
        {
            get
            {
                if (HasPath)
                    return System.IO.Path.Combine(Path, FileName);
                else
                    return FileName;
            }
        }

        internal bool HasPath => !(String.IsNullOrWhiteSpace(Path));

        public bool FileExists => File.Exists(FullFileName);

        public FileConnectionManager(string filename)
        {
            this.FileName = filename;
        }

        public FileConnectionManager(string path, string filename) : this(filename)
        {
            this.Path = path;
        }

        internal string ReadContent()
        {
            if (FileExists)
                return File.ReadAllText(FullFileName);
            else
                return String.Empty;
        }
    }
}
