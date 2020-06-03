using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;


namespace ETLBoxTests.Helper
{
    public static class TestHashHelper
    {
        public static string RandomString(int length)
        {
            var random = new Random();
            const string pool = "abcdefghijklmnopqrstuvwxyz0123456789";
            var chars = Enumerable.Range(0, length)
                .Select(x => pool[random.Next(0, pool.Length)]);
            return new string(chars.ToArray());
        }

    }
}
