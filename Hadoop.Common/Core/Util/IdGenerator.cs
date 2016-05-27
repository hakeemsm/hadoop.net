using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// Generic ID generator
	/// used for generating various types of number sequences.
	/// </summary>
	public interface IdGenerator
	{
		/// <summary>Increment and then return the next value.</summary>
		long NextValue();
	}
}
