using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class implements the sort method from BasicTypeSorterBase class as
	/// MergeSort.
	/// </summary>
	/// <remarks>
	/// This class implements the sort method from BasicTypeSorterBase class as
	/// MergeSort. Note that this class is really a wrapper over the actual
	/// mergesort implementation that is there in the util package. The main intent
	/// of providing this class is to setup the input data for the util.MergeSort
	/// algo so that the latter doesn't need to bother about the various data
	/// structures that have been created for the Map output but rather concentrate
	/// on the core algorithm (thereby allowing easy integration of a mergesort
	/// implementation). The bridge between this class and the util.MergeSort class
	/// is the Comparator.
	/// </remarks>
	internal class MergeSorter : BasicTypeSorterBase, IComparer<IntWritable>
	{
		private static int progressUpdateFrequency = 10000;

		private int progressCalls = 0;

		/// <summary>The sort method derived from BasicTypeSorterBase and overridden here</summary>
		public override SequenceFile.Sorter.RawKeyValueIterator Sort()
		{
			MergeSort m = new MergeSort(this);
			int count = base.count;
			if (count == 0)
			{
				return null;
			}
			int[] pointers = base.pointers;
			int[] pointersCopy = new int[count];
			System.Array.Copy(pointers, 0, pointersCopy, 0, count);
			m.MergeSort(pointers, pointersCopy, 0, count);
			return new MRSortResultIterator(base.keyValBuffer, pointersCopy, base.startOffsets
				, base.keyLengths, base.valueLengths);
		}

		/// <summary>The implementation of the compare method from Comparator.</summary>
		/// <remarks>
		/// The implementation of the compare method from Comparator. Note that
		/// Comparator.compare takes objects as inputs and so the int values are
		/// wrapped in (reusable) IntWritables from the class util.MergeSort
		/// </remarks>
		/// <param name="i"/>
		/// <param name="j"/>
		/// <returns>int as per the specification of Comparator.compare</returns>
		public virtual int Compare(IntWritable i, IntWritable j)
		{
			// indicate we're making progress but do a batch update
			if (progressCalls < progressUpdateFrequency)
			{
				progressCalls++;
			}
			else
			{
				progressCalls = 0;
				reporter.Progress();
			}
			return comparator.Compare(keyValBuffer.GetData(), startOffsets[i.Get()], keyLengths
				[i.Get()], keyValBuffer.GetData(), startOffsets[j.Get()], keyLengths[j.Get()]);
		}

		/// <summary>Add the extra memory that will be utilized by the sort method</summary>
		public override long GetMemoryUtilized()
		{
			//this is memory that will be actually utilized (considering the temp
			//array that will be allocated by the sort() method (mergesort))
			return base.GetMemoryUtilized() + base.count * 4;
		}
	}
}
