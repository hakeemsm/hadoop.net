using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>
	/// Generic ID generator
	/// used for generating various types of number sequences.
	/// </summary>
	public interface IdGenerator
	{
		/// <summary>Increment and then return the next value.</summary>
		long nextValue();
	}
}
