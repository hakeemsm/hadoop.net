using Sharpen;

namespace org.apache.hadoop.cli.util
{
	public class SubstringComparator : org.apache.hadoop.cli.util.ComparatorBase
	{
		public override bool compare(string actual, string expected)
		{
			int compareOutput = actual.IndexOf(expected);
			if (compareOutput == -1)
			{
				return false;
			}
			return true;
		}
	}
}
