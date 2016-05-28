using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI
{
	/// <summary>A class is Combinable if its object can be combined with other objects.</summary>
	/// <?/>
	public interface Combinable<T> : Comparable<T>
	{
		/// <summary>Combine this with that.</summary>
		/// <param name="that">Another object.</param>
		/// <returns>The combined object.</returns>
		T Combine(T that);
	}
}
