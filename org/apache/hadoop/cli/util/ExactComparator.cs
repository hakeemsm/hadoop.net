using Sharpen;

namespace org.apache.hadoop.cli.util
{
	/// <summary>Comparator for the Command line tests.</summary>
	/// <remarks>
	/// Comparator for the Command line tests.
	/// This comparator compares the actual to the expected and
	/// returns true only if they are the same
	/// </remarks>
	public class ExactComparator : org.apache.hadoop.cli.util.ComparatorBase
	{
		public override bool compare(string actual, string expected)
		{
			return actual.Equals(expected);
		}
	}
}
