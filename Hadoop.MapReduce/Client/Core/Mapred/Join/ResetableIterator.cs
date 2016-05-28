using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Lib.Join;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>
	/// This defines an interface to a stateful Iterator that can replay elements
	/// added to it directly.
	/// </summary>
	/// <remarks>
	/// This defines an interface to a stateful Iterator that can replay elements
	/// added to it directly.
	/// Note that this does not extend
	/// <see cref="System.Collections.IEnumerator{E}"/>
	/// .
	/// </remarks>
	public abstract class ResetableIterator<T> : ResetableIterator<T>
		where T : Writable
	{
		public class EMPTY<U> : ResetableIterator.EMPTY<U>, ResetableIterator<U>
			where U : Writable
		{
		}
	}

	public static class ResetableIteratorConstants
	{
	}
}
