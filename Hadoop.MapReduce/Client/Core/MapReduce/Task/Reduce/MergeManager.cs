using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	/// <summary>
	/// An interface for a reduce side merge that works with the default Shuffle
	/// implementation.
	/// </summary>
	public interface MergeManager<K, V>
	{
		/// <summary>
		/// To wait until merge has some freed resources available so that it can
		/// accept shuffled data.
		/// </summary>
		/// <remarks>
		/// To wait until merge has some freed resources available so that it can
		/// accept shuffled data.  This will be called before a network connection is
		/// established to get the map output.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		void WaitForResource();

		/// <summary>To reserve resources for data to be shuffled.</summary>
		/// <remarks>
		/// To reserve resources for data to be shuffled.  This will be called after
		/// a network connection is made to shuffle the data.
		/// </remarks>
		/// <param name="mapId">mapper from which data will be shuffled.</param>
		/// <param name="requestedSize">size in bytes of data that will be shuffled.</param>
		/// <param name="fetcher">id of the map output fetcher that will shuffle the data.</param>
		/// <returns>
		/// a MapOutput object that can be used by shuffle to shuffle data.  If
		/// required resources cannot be reserved immediately, a null can be returned.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		MapOutput<K, V> Reserve(TaskAttemptID mapId, long requestedSize, int fetcher);

		/// <summary>Called at the end of shuffle.</summary>
		/// <returns>a key value iterator object.</returns>
		/// <exception cref="System.Exception"/>
		RawKeyValueIterator Close();
	}
}
