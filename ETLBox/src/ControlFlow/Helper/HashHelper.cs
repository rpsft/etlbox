using ETLBox.ControlFlow;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;


namespace ETLBox.Helper
{
    /// <summary>
    /// This class creates unique strings containing hash values.
    /// </summary>
    public static class HashHelper
    {
        /// <summary>
        /// Creates a 40 character unique hash string
        /// </summary>
        /// <param name="text">Text that needs to be hashed</param>
        /// <returns>A unique readable hash string with 40 characters</returns>
        public static string CreateChar40Hash(string text)
        {
            if (text != null)
            {
                string hex = "";
                byte[] hashValue = new SHA1Managed().ComputeHash(Encoding.UTF8.GetBytes(text));
                foreach (byte hashByte in hashValue)
                    hex += hashByte.ToString("x2");
                return hex.ToUpper();
            }
            else
                return "";
        }

        /// <summary>
        /// Creates a unique hash string from a loggable task
        /// </summary>
        /// <param name="task">The ETLBox loggable task</param>
        /// <returns>A unique readable hash string with 40 character</returns>
        public static string CreateChar40Hash(ILoggableTask task) => CreateChar40Hash(task.TaskName + "|" + task.TaskType + "|" + task.GetHashCode());

        /// <summary>
        /// Addition of the hash values for a list of object
        /// </summary>
        /// <param name="objectList">A list of objects</param>
        /// <returns>A unique hash value</returns>
        public static int HashSum(IEnumerable<object> objectList)
        {
            unchecked
            {
                int hash = 29;
                foreach (var o in objectList)
                    hash = hash * 486187739 + (o?.GetHashCode() ?? 17);
                return hash;
            }
        }
    }
}
