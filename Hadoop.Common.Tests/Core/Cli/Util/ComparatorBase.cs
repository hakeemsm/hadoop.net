using Sharpen;

namespace Org.Apache.Hadoop.Cli.Util
{
	/// <summary>Comparator interface.</summary>
	/// <remarks>
	/// Comparator interface. To define a new comparator, implement the compare
	/// method
	/// </remarks>
	public abstract class ComparatorBase
	{
		public ComparatorBase()
		{
		}

		/// <summary>Compare method for the comparator class.</summary>
		/// <param name="actual">output. can be null</param>
		/// <param name="expected">output. can be null</param>
		/// <returns>
		/// true if expected output compares with the actual output, else
		/// return false. If actual or expected is null, return false
		/// </returns>
		public abstract bool Compare(string actual, string expected);
	}
}
