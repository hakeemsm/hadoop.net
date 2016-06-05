using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI
{
	/// <summary>A class is a Container if it contains an element.</summary>
	/// <?/>
	public interface Container<T>
	{
		/// <returns>The contained element.</returns>
		T GetElement();
	}
}
