using Sharpen;

namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>Comparator for the Command line tests.</summary>
	/// <remarks>
	/// Comparator for the Command line tests.
	/// This comparator compares the actual to the expected and
	/// returns true only if they are the same
	/// </remarks>
	public class ExactComparator : ComparatorBase
	{
		public override bool Compare(string actual, string expected)
		{
			return actual.Equals(expected);
		}
	}
}
