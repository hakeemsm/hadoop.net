using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	/// <summary>This class provides an implementation of ResetableIterator.</summary>
	/// <remarks>
	/// This class provides an implementation of ResetableIterator. The
	/// implementation uses an
	/// <see cref="System.Collections.ArrayList{E}"/>
	/// to store elements
	/// added to it, replaying them as requested.
	/// Prefer
	/// <see cref="StreamBackedIterator{X}"/>
	/// .
	/// </remarks>
	public class ArrayListBackedIterator<X> : Org.Apache.Hadoop.Mapreduce.Lib.Join.ArrayListBackedIterator
		<X>, ResetableIterator<X>
		where X : Writable
	{
		public ArrayListBackedIterator()
			: base()
		{
		}

		public ArrayListBackedIterator(AList<X> data)
			: base(data)
		{
		}
	}
}
