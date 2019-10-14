using System.IO;
using System.Text.RegularExpressions;

namespace ALE.ETLBox
{
    public class ExtensionFile
    {
        public const string VERSIONMATCH = @"--\W*@version\W*:\W*[><=]?(\d+.\d+)\W*([VFvf][Pp]\d+)?";
        public const string SKIPNEXT = @"--\W*@skipnext\W*:\W*[Tt]rue";
        public const string FILENAMEMATCH = @"^(\w*?)_([0-9A-Za-z!%&()=+#~äöüÄÖÜß -]*).sql";

        public string Type { get; private set; }
        public string Name { get; private set; }

        public string Content { get; private set; }

        public string FileName { get; set; }

        public bool IsStagExtension => Type.ToLower() == ExtensionFileLoader.STAGEXT.ToLower();

        public bool IsValidExtension { get; private set; } = true;
        public bool HasSkipNextStatement { get; private set; }

        public ExtensionFile(string filename)
        {
            this.FileName = filename;

            FillNameAndType();
            ReadContent();

            CheckIfHasVersion();
            CheckIfHasSkipNext();
        }



        private void FillNameAndType()
        {
            string fileName = FileName.Substring(FileName.LastIndexOf(@"\") + 1);
            Match m = Regex.Match(fileName, FILENAMEMATCH);
            if (m.Success)
            {
                Type = m.Groups[1].Value;
                Name = m.Groups[2].Value;
            }
            else
            {
                IsValidExtension = false;
            }
        }

        public void ReadContent()
        {
            Content = File.ReadAllText(FileName);
        }

        public void CheckIfHasVersion()
        {
            if (!Regex.IsMatch(Content, VERSIONMATCH)) IsValidExtension = false;
        }
        public void CheckIfHasSkipNext()
        {
            HasSkipNextStatement = Regex.IsMatch(Content, SKIPNEXT) ? true : false;
        }

    }
}
