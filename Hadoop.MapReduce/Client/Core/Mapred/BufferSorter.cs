using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class provides a generic sort interface that should be implemented
	/// by specific sort algorithms.
	/// </summary>
	/// <remarks>
	/// This class provides a generic sort interface that should be implemented
	/// by specific sort algorithms. The use case is the following:
	/// A user class writes key/value records to a buffer, and finally wants to
	/// sort the buffer. This interface defines methods by which the user class
	/// can update the interface implementation with the offsets of the records
	/// and the lengths of the keys/values. The user class gives a reference to
	/// the buffer when the latter wishes to sort the records written to the buffer
	/// so far. Typically, the user class decides the point at which sort should
	/// happen based on the memory consumed so far by the buffer and the data
	/// structures maintained by an implementation of this interface. That is why
	/// a method is provided to get the memory consumed so far by the datastructures
	/// in the interface implementation.
	/// </remarks>
	internal interface BufferSorter : JobConfigurable
	{
		/// <summary>Pass the Progressable object so that sort can call progress while it is sorting
		/// 	</summary>
		/// <param name="reporter">the Progressable object reference</param>
		void SetProgressable(Progressable reporter);

		/// <summary>
		/// When a key/value is added at a particular offset in the key/value buffer,
		/// this method is invoked by the user class so that the impl of this sort
		/// interface can update its datastructures.
		/// </summary>
		/// <param name="recordOffset">the offset of the key in the buffer</param>
		/// <param name="keyLength">the length of the key</param>
		/// <param name="valLength">the length of the val in the buffer</param>
		void AddKeyValue(int recordoffset, int keyLength, int valLength);

		/// <summary>
		/// The user class invokes this method to set the buffer that the specific
		/// sort algorithm should "indirectly" sort (generally, sort algorithm impl
		/// should access this buffer via comparators and sort offset-indices to the
		/// buffer).
		/// </summary>
		/// <param name="buffer">the map output buffer</param>
		void SetInputBuffer(OutputBuffer buffer);

		/// <summary>
		/// The framework invokes this method to get the memory consumed so far
		/// by an implementation of this interface.
		/// </summary>
		/// <returns>memoryUsed in bytes</returns>
		long GetMemoryUtilized();

		/// <summary>Framework decides when to actually sort</summary>
		SequenceFile.Sorter.RawKeyValueIterator Sort();

		/// <summary>Framework invokes this to signal the sorter to cleanup</summary>
		void Close();
	}
}
