namespace ALE.ETLBox.Helper {
    public static class StringExtension {
        public static string NullOrSqlString(this string s) => s == null ? "null" : $"'{s.Replace("'","''")}'";

		/// <summary>
		///		This replicates the functionality of case-insensitive functionality built into Replace in .Net Core.
		/// </summary>
		/// <param name="toSearch"></param>
		/// <param name="find"></param>
		/// <param name="replace"></param>
		/// <returns></returns>
		public static string ReplaceIgnoreCase(this string toSearch, string find, string replace)
		{
			int index = toSearch.IndexOf(
				find,
				System.StringComparison.InvariantCultureIgnoreCase);

			if (index < 0)
			{
				return toSearch;
			}

			string replacement = toSearch.Substring(0, index) + replace;

			if (toSearch.Length > index + find.Length)
			{
				replacement += toSearch.Substring(index + find.Length);
			}

			return replacement;
		}
	}
}
