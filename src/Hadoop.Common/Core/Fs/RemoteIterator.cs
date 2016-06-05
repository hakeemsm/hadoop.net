

namespace Org.Apache.Hadoop.FS
{
	/// <summary>An iterator over a collection whose elements need to be fetched remotely
	/// 	</summary>
	public interface RemoteIterator<E>
	{
		/// <summary>Returns <tt>true</tt> if the iteration has more elements.</summary>
		/// <returns><tt>true</tt> if the iterator has more elements.</returns>
		/// <exception cref="System.IO.IOException">if any IO error occurs</exception>
		bool HasNext();

		/// <summary>Returns the next element in the iteration.</summary>
		/// <returns>the next element in the iteration.</returns>
		/// <exception cref="NoSuchElementException">iteration has no more elements.</exception>
		/// <exception cref="System.IO.IOException">if any IO error occurs</exception>
		E Next();
	}
}
