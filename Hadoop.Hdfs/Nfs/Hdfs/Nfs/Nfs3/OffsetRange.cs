using System.Collections.Generic;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>OffsetRange is the range of read/write request.</summary>
	/// <remarks>
	/// OffsetRange is the range of read/write request. A single point (e.g.,[5,5])
	/// is not a valid range.
	/// </remarks>
	public class OffsetRange
	{
		private sealed class _IComparer_31 : IComparer<Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.OffsetRange
			>
		{
			public _IComparer_31()
			{
			}

			public int Compare(Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.OffsetRange o1, Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.OffsetRange
				 o2)
			{
				if (o1.GetMin() == o2.GetMin())
				{
					return o1.GetMax() < o2.GetMax() ? 1 : (o1.GetMax() > o2.GetMax() ? -1 : 0);
				}
				else
				{
					return o1.GetMin() < o2.GetMin() ? 1 : -1;
				}
			}
		}

		public static readonly IComparer<Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.OffsetRange> ReverseComparatorOnMin
			 = new _IComparer_31();

		private readonly long min;

		private readonly long max;

		internal OffsetRange(long min, long max)
		{
			Preconditions.CheckArgument(min >= 0 && max >= 0 && min < max);
			this.min = min;
			this.max = max;
		}

		internal virtual long GetMin()
		{
			return min;
		}

		internal virtual long GetMax()
		{
			return max;
		}

		public override int GetHashCode()
		{
			return (int)(min ^ max);
		}

		public override bool Equals(object o)
		{
			if (o is Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.OffsetRange)
			{
				Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.OffsetRange range = (Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.OffsetRange
					)o;
				return (min == range.GetMin()) && (max == range.GetMax());
			}
			return false;
		}
	}
}
