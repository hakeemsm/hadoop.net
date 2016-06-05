

namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>Comparator for the Command line tests.</summary>
	/// <remarks>
	/// Comparator for the Command line tests.
	/// This comparator searches for the regular expression specified in 'expected'
	/// in the string 'actual' and returns true if the regular expression match is
	/// done
	/// </remarks>
	public class RegexpComparator : ComparatorBase
	{
		public override bool Compare(string actual, string expected)
		{
			bool success = false;
			Pattern p = Pattern.Compile(expected);
			StringTokenizer tokenizer = new StringTokenizer(actual, "\n\r");
			while (tokenizer.HasMoreTokens() && !success)
			{
				string actualToken = tokenizer.NextToken();
				Matcher m = p.Matcher(actualToken);
				success = m.Matches();
			}
			return success;
		}
	}
}
