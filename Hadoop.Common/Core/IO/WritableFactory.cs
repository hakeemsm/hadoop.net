using Hadoop.Common.Core.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A factory for a class of Writable.</summary>
	/// <seealso cref="WritableFactories"/>
	public interface WritableFactory
	{
		/// <summary>Return a new instance.</summary>
		IWritable NewInstance();
	}
}
