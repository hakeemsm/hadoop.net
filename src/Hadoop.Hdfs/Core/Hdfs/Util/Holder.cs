using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>A Holder is simply a wrapper around some other object.</summary>
	/// <remarks>
	/// A Holder is simply a wrapper around some other object. This is useful
	/// in particular for storing immutable values like boxed Integers in a
	/// collection without having to do the &quot;lookup&quot; of the value twice.
	/// </remarks>
	public class Holder<T>
	{
		public T held;

		public Holder(T held)
		{
			this.held = held;
		}

		public override string ToString()
		{
			return held.ToString();
		}
	}
}
