using Sharpen;

namespace org.apache.hadoop.cli.util
{
	/// <summary>Comparator for the Command line tests.</summary>
	/// <remarks>
	/// Comparator for the Command line tests.
	/// This comparator searches for the regular expression specified in 'expected'
	/// in the string 'actual' and returns true if the regular expression match is
	/// done
	/// </remarks>
	public class RegexpComparator : org.apache.hadoop.cli.util.ComparatorBase
	{
		public override bool compare(string actual, string expected)
		{
			bool success = false;
			java.util.regex.Pattern p = java.util.regex.Pattern.compile(expected);
			java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(actual, "\n\r"
				);
			while (tokenizer.hasMoreTokens() && !success)
			{
				string actualToken = tokenizer.nextToken();
				java.util.regex.Matcher m = p.matcher(actualToken);
				success = m.matches();
			}
			return success;
		}
	}
}
