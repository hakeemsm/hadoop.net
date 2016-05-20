using Sharpen;

namespace org.apache.hadoop.cli.util
{
	/// <summary>Comparator for the Command line tests.</summary>
	/// <remarks>
	/// Comparator for the Command line tests.
	/// This comparator compares each token in the expected output and returns true
	/// if all tokens are in the actual output
	/// </remarks>
	public class TokenComparator : org.apache.hadoop.cli.util.ComparatorBase
	{
		public override bool compare(string actual, string expected)
		{
			bool compareOutput = true;
			java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(expected, ",\n\r"
				);
			while (tokenizer.hasMoreTokens())
			{
				string token = tokenizer.nextToken();
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
