

namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>Comparator for the Command line tests.</summary>
	/// <remarks>
	/// Comparator for the Command line tests.
	/// This comparator compares each token in the expected output and returns true
	/// if all tokens are in the actual output
	/// </remarks>
	public class TokenComparator : ComparatorBase
	{
		public override bool Compare(string actual, string expected)
		{
			bool compareOutput = true;
			StringTokenizer tokenizer = new StringTokenizer(expected, ",\n\r");
			while (tokenizer.HasMoreTokens())
			{
				string token = tokenizer.NextToken();
				if (actual.IndexOf(token) != -1)
				{
					compareOutput &= true;
				}
				else
				{
					compareOutput &= false;
				}
			}
			return compareOutput;
		}
	}
}
