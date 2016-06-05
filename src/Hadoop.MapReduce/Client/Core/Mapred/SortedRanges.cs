using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Keeps the Ranges sorted by startIndex.</summary>
	/// <remarks>
	/// Keeps the Ranges sorted by startIndex.
	/// The added ranges are always ensured to be non-overlapping.
	/// Provides the SkipRangeIterator, which skips the Ranges
	/// stored in this object.
	/// </remarks>
	internal class SortedRanges : Writable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(SortedRanges));

		private TreeSet<SortedRanges.Range> ranges = new TreeSet<SortedRanges.Range>();

		private long indicesCount;

		/// <summary>Get Iterator which skips the stored ranges.</summary>
		/// <remarks>
		/// Get Iterator which skips the stored ranges.
		/// The Iterator.next() call return the index starting from 0.
		/// </remarks>
		/// <returns>SkipRangeIterator</returns>
		internal virtual SortedRanges.SkipRangeIterator SkipRangeIterator()
		{
			lock (this)
			{
				return new SortedRanges.SkipRangeIterator(ranges.GetEnumerator());
			}
		}

		/// <summary>Get the no of indices stored in the ranges.</summary>
		/// <returns>indices count</returns>
		internal virtual long GetIndicesCount()
		{
			lock (this)
			{
				return indicesCount;
			}
		}

		/// <summary>Get the sorted set of ranges.</summary>
		/// <returns>ranges</returns>
		internal virtual ICollection<SortedRanges.Range> GetRanges()
		{
			lock (this)
			{
				return ranges;
			}
		}

		/// <summary>Add the range indices.</summary>
		/// <remarks>
		/// Add the range indices. It is ensured that the added range
		/// doesn't overlap the existing ranges. If it overlaps, the
		/// existing overlapping ranges are removed and a single range
		/// having the superset of all the removed ranges and this range
		/// is added.
		/// If the range is of 0 length, doesn't do anything.
		/// </remarks>
		/// <param name="range">Range to be added.</param>
		internal virtual void Add(SortedRanges.Range range)
		{
			lock (this)
			{
				if (range.IsEmpty())
				{
					return;
				}
				long startIndex = range.GetStartIndex();
				long endIndex = range.GetEndIndex();
				//make sure that there are no overlapping ranges
				ICollection<SortedRanges.Range> headSet = ranges.HeadSet(range);
				if (headSet.Count > 0)
				{
					SortedRanges.Range previousRange = headSet.Last();
					Log.Debug("previousRange " + previousRange);
					if (startIndex < previousRange.GetEndIndex())
					{
						//previousRange overlaps this range
						//remove the previousRange
						if (ranges.Remove(previousRange))
						{
							indicesCount -= previousRange.GetLength();
						}
						//expand this range
						startIndex = previousRange.GetStartIndex();
						endIndex = endIndex >= previousRange.GetEndIndex() ? endIndex : previousRange.GetEndIndex
							();
					}
				}
				IEnumerator<SortedRanges.Range> tailSetIt = ranges.TailSet(range).GetEnumerator();
				while (tailSetIt.HasNext())
				{
					SortedRanges.Range nextRange = tailSetIt.Next();
					Log.Debug("nextRange " + nextRange + "   startIndex:" + startIndex + "  endIndex:"
						 + endIndex);
					if (endIndex >= nextRange.GetStartIndex())
					{
						//nextRange overlaps this range
						//remove the nextRange
						tailSetIt.Remove();
						indicesCount -= nextRange.GetLength();
						if (endIndex < nextRange.GetEndIndex())
						{
							//expand this range
							endIndex = nextRange.GetEndIndex();
							break;
						}
					}
					else
					{
						break;
					}
				}
				Add(startIndex, endIndex);
			}
		}

		/// <summary>Remove the range indices.</summary>
		/// <remarks>
		/// Remove the range indices. If this range is
		/// found in existing ranges, the existing ranges
		/// are shrunk.
		/// If range is of 0 length, doesn't do anything.
		/// </remarks>
		/// <param name="range">Range to be removed.</param>
		internal virtual void Remove(SortedRanges.Range range)
		{
			lock (this)
			{
				if (range.IsEmpty())
				{
					return;
				}
				long startIndex = range.GetStartIndex();
				long endIndex = range.GetEndIndex();
				//make sure that there are no overlapping ranges
				ICollection<SortedRanges.Range> headSet = ranges.HeadSet(range);
				if (headSet.Count > 0)
				{
					SortedRanges.Range previousRange = headSet.Last();
					Log.Debug("previousRange " + previousRange);
					if (startIndex < previousRange.GetEndIndex())
					{
						//previousRange overlaps this range
						//narrow down the previousRange
						if (ranges.Remove(previousRange))
						{
							indicesCount -= previousRange.GetLength();
							Log.Debug("removed previousRange " + previousRange);
						}
						Add(previousRange.GetStartIndex(), startIndex);
						if (endIndex <= previousRange.GetEndIndex())
						{
							Add(endIndex, previousRange.GetEndIndex());
						}
					}
				}
				IEnumerator<SortedRanges.Range> tailSetIt = ranges.TailSet(range).GetEnumerator();
				while (tailSetIt.HasNext())
				{
					SortedRanges.Range nextRange = tailSetIt.Next();
					Log.Debug("nextRange " + nextRange + "   startIndex:" + startIndex + "  endIndex:"
						 + endIndex);
					if (endIndex > nextRange.GetStartIndex())
					{
						//nextRange overlaps this range
						//narrow down the nextRange
						tailSetIt.Remove();
						indicesCount -= nextRange.GetLength();
						if (endIndex < nextRange.GetEndIndex())
						{
							Add(endIndex, nextRange.GetEndIndex());
							break;
						}
					}
					else
					{
						break;
					}
				}
			}
		}

		private void Add(long start, long end)
		{
			if (end > start)
			{
				SortedRanges.Range recRange = new SortedRanges.Range(start, end - start);
				ranges.AddItem(recRange);
				indicesCount += recRange.GetLength();
				Log.Debug("added " + recRange);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			lock (this)
			{
				indicesCount = @in.ReadLong();
				ranges = new TreeSet<SortedRanges.Range>();
				int size = @in.ReadInt();
				for (int i = 0; i < size; i++)
				{
					SortedRanges.Range range = new SortedRanges.Range();
					range.ReadFields(@in);
					ranges.AddItem(range);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			lock (this)
			{
				@out.WriteLong(indicesCount);
				@out.WriteInt(ranges.Count);
				IEnumerator<SortedRanges.Range> it = ranges.GetEnumerator();
				while (it.HasNext())
				{
					SortedRanges.Range range = it.Next();
					range.Write(@out);
				}
			}
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			IEnumerator<SortedRanges.Range> it = ranges.GetEnumerator();
			while (it.HasNext())
			{
				SortedRanges.Range range = it.Next();
				sb.Append(range.ToString() + "\n");
			}
			return sb.ToString();
		}

		/// <summary>Index Range.</summary>
		/// <remarks>
		/// Index Range. Comprises of start index and length.
		/// A Range can be of 0 length also. The Range stores indices
		/// of type long.
		/// </remarks>
		internal class Range : Comparable<SortedRanges.Range>, Writable
		{
			private long startIndex;

			private long length;

			internal Range(long startIndex, long length)
			{
				if (length < 0)
				{
					throw new RuntimeException("length can't be negative");
				}
				this.startIndex = startIndex;
				this.length = length;
			}

			internal Range()
				: this(0, 0)
			{
			}

			/// <summary>Get the start index.</summary>
			/// <remarks>Get the start index. Start index in inclusive.</remarks>
			/// <returns>startIndex.</returns>
			internal virtual long GetStartIndex()
			{
				return startIndex;
			}

			/// <summary>Get the end index.</summary>
			/// <remarks>Get the end index. End index is exclusive.</remarks>
			/// <returns>endIndex.</returns>
			internal virtual long GetEndIndex()
			{
				return startIndex + length;
			}

			/// <summary>Get Length.</summary>
			/// <returns>length</returns>
			internal virtual long GetLength()
			{
				return length;
			}

			/// <summary>Range is empty if its length is zero.</summary>
			/// <returns>
			/// <code>true</code> if empty
			/// <code>false</code> otherwise.
			/// </returns>
			internal virtual bool IsEmpty()
			{
				return length == 0;
			}

			public override bool Equals(object o)
			{
				if (o is SortedRanges.Range)
				{
					SortedRanges.Range range = (SortedRanges.Range)o;
					return startIndex == range.startIndex && length == range.length;
				}
				return false;
			}

			public override int GetHashCode()
			{
				return Sharpen.Extensions.ValueOf(startIndex).GetHashCode() + Sharpen.Extensions.ValueOf
					(length).GetHashCode();
			}

			public virtual int CompareTo(SortedRanges.Range o)
			{
				// Ensure sgn(x.compareTo(y) == -sgn(y.compareTo(x))
				return this.startIndex < o.startIndex ? -1 : (this.startIndex > o.startIndex ? 1 : 
					(this.length < o.length ? -1 : (this.length > o.length ? 1 : 0)));
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				startIndex = @in.ReadLong();
				length = @in.ReadLong();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteLong(startIndex);
				@out.WriteLong(length);
			}

			public override string ToString()
			{
				return startIndex + ":" + length;
			}
		}

		/// <summary>Index Iterator which skips the stored ranges.</summary>
		internal class SkipRangeIterator : IEnumerator<long>
		{
			internal IEnumerator<SortedRanges.Range> rangeIterator;

			internal SortedRanges.Range range = new SortedRanges.Range();

			internal long next = -1;

			/// <summary>Constructor</summary>
			/// <param name="rangeIterator">the iterator which gives the ranges.</param>
			internal SkipRangeIterator(IEnumerator<SortedRanges.Range> rangeIterator)
			{
				this.rangeIterator = rangeIterator;
				DoNext();
			}

			/// <summary>Returns true till the index reaches Long.MAX_VALUE.</summary>
			/// <returns>
			/// <code>true</code> next index exists.
			/// <code>false</code> otherwise.
			/// </returns>
			public override bool HasNext()
			{
				lock (this)
				{
					return next < long.MaxValue;
				}
			}

			/// <summary>Get the next available index.</summary>
			/// <remarks>Get the next available index. The index starts from 0.</remarks>
			/// <returns>next index</returns>
			public override long Next()
			{
				lock (this)
				{
					long ci = next;
					DoNext();
					return ci;
				}
			}

			private void DoNext()
			{
				next++;
				Log.Debug("currentIndex " + next + "   " + range);
				SkipIfInRange();
				while (next >= range.GetEndIndex() && rangeIterator.HasNext())
				{
					range = rangeIterator.Next();
					SkipIfInRange();
				}
			}

			private void SkipIfInRange()
			{
				if (next >= range.GetStartIndex() && next < range.GetEndIndex())
				{
					//need to skip the range
					Log.Warn("Skipping index " + next + "-" + range.GetEndIndex());
					next = range.GetEndIndex();
				}
			}

			/// <summary>Get whether all the ranges have been skipped.</summary>
			/// <returns>
			/// <code>true</code> if all ranges have been skipped.
			/// <code>false</code> otherwise.
			/// </returns>
			internal virtual bool SkippedAllRanges()
			{
				lock (this)
				{
					return !rangeIterator.HasNext() && next > range.GetEndIndex();
				}
			}

			/// <summary>Remove is not supported.</summary>
			/// <remarks>Remove is not supported. Doesn't apply.</remarks>
			public override void Remove()
			{
				throw new NotSupportedException("remove not supported.");
			}
		}
	}
}
