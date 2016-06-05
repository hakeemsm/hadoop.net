using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Lib.Join;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>This class provides an implementation of ResetableIterator.</summary>
	/// <remarks>
	/// This class provides an implementation of ResetableIterator. This
	/// implementation uses a byte array to store elements added to it.
	/// </remarks>
	public class StreamBackedIterator<X> : StreamBackedIterator<X>, ResetableIterator
		<X>
		where X : Writable
	{
	}
}
