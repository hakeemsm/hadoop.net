using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>Create a set of buckets that hold key-time pairs.</summary>
	/// <remarks>
	/// Create a set of buckets that hold key-time pairs. When the values of the
	/// buckets is queried, the number of objects with time differences in the
	/// different buckets is returned.
	/// </remarks>
	internal class TimeBucketMetrics<Obj>
	{
		private readonly Dictionary<OBJ, long> map = new Dictionary<OBJ, long>();

		private readonly int[] counts;

		private readonly long[] cuts;

		/// <summary>Create a set of buckets based on a set of time points.</summary>
		/// <remarks>
		/// Create a set of buckets based on a set of time points. The number of
		/// buckets is one more than the number of points.
		/// </remarks>
		internal TimeBucketMetrics(long[] cuts)
		{
			this.cuts = cuts;
			counts = new int[cuts.Length + 1];
		}

		/// <summary>Add an object to be counted</summary>
		internal virtual void Add(OBJ key, long time)
		{
			lock (this)
			{
				map[key] = time;
			}
		}

		/// <summary>Remove an object to be counted</summary>
		internal virtual void Remove(OBJ key)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(map, key);
			}
		}

		/// <summary>Find the bucket based on the cut points.</summary>
		private int FindBucket(long val)
		{
			for (int i = 0; i < cuts.Length; ++i)
			{
				if (val < cuts[i])
				{
					return i;
				}
			}
			return cuts.Length;
		}

		/// <summary>Get the counts of how many keys are in each bucket.</summary>
		/// <remarks>
		/// Get the counts of how many keys are in each bucket. The same array is
		/// returned by each call to this method.
		/// </remarks>
		internal virtual int[] GetBucketCounts(long now)
		{
			lock (this)
			{
				for (int i = 0; i < counts.Length; ++i)
				{
					counts[i] = 0;
				}
				foreach (long time in map.Values)
				{
					counts[FindBucket(now - time)] += 1;
				}
				return counts;
			}
		}
	}
}
