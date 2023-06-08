using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace ALE.ETLBox.Helper
{
    public static class HashHelper
    {
        public static string Encrypt_Char40(string text)
        {
            if (text != null)
            {
                string hex = "";
                byte[] hashValue = new SHA1Managed().ComputeHash(Encoding.UTF8.GetBytes(text));
                foreach (byte hashByte in hashValue)
                    hex += hashByte.ToString("x2");
                return hex.ToUpper();
            }

            return "";
        }

        public static string Encrypt_Char40(ITask task) =>
            Encrypt_Char40(task.TaskName + "|" + task.TaskType);

        public static string Encrypt_Char40(ITask task, string id) =>
            Encrypt_Char40(task.TaskName + "|" + task.TaskType + "|" + id);

        public static string RandomString(int length)
        {
            var random = new Random();
            const string pool = "abcdefghijklmnopqrstuvwxyz0123456789";
            var chars = Enumerable.Range(0, length).Select(_ => pool[random.Next(0, pool.Length)]);
            return new string(chars.ToArray());
        }
    }
}
