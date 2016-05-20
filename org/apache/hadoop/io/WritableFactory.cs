using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A factory for a class of Writable.</summary>
	/// <seealso cref="WritableFactories"/>
	public interface WritableFactory
	{
		/// <summary>Return a new instance.</summary>
		org.apache.hadoop.io.Writable newInstance();
	}
}
