using Sharpen;

namespace Org.Apache.Hadoop.Cli.Util
{
	public class SubstringComparator : ComparatorBase
	{
		public override bool Compare(string actual, string expected)
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
