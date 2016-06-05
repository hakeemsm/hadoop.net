using System;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Window
{
	/// <summary>A class for exposing a rolling window view on the event that occur over time.
	/// 	</summary>
	/// <remarks>
	/// A class for exposing a rolling window view on the event that occur over time.
	/// Events are reported based on occurrence time. The total number of events in
	/// the last period covered by the rolling window can be retrieved by the
	/// <see cref="GetSum(long)"/>
	/// method.
	/// <p/>
	/// Assumptions:
	/// <p/>
	/// (1) Concurrent invocation of
	/// <see cref="IncAt(long, long)"/>
	/// method are possible
	/// <p/>
	/// (2) The time parameter of two consecutive invocation of
	/// <see cref="IncAt(long, long)"/>
	/// could
	/// be in any given order
	/// <p/>
	/// (3) The buffering delays are not more than the window length, i.e., after two
	/// consecutive invocation
	/// <see>#incAt(long time1, long)</see>
	/// and
	/// <see>#incAt(long time2, long)</see>
	/// , time1 &lt; time2 || time1 - time2 &lt; windowLenMs.
	/// This assumption helps avoiding unnecessary synchronizations.
	/// <p/>
	/// Thread-safety is built in the
	/// <see cref="Bucket"/>
	/// </remarks>
	public class RollingWindow
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Window.RollingWindow
			));

		/// <summary>
		/// Each window is composed of buckets, which offer a trade-off between
		/// accuracy and space complexity: the lower the number of buckets, the less
		/// memory is required by the rolling window but more inaccuracy is possible in
		/// reading window total values.
		/// </summary>
		internal RollingWindow.Bucket[] buckets;

		internal readonly int windowLenMs;

		internal readonly int bucketSize;

		/// <param name="windowLenMs">
		/// The period that is covered by the window. This period must
		/// be more than the buffering delays.
		/// </param>
		/// <param name="numBuckets">number of buckets in the window</param>
		internal RollingWindow(int windowLenMs, int numBuckets)
		{
			buckets = new RollingWindow.Bucket[numBuckets];
			for (int i = 0; i < numBuckets; i++)
			{
				buckets[i] = new RollingWindow.Bucket(this);
			}
			this.windowLenMs = windowLenMs;
			this.bucketSize = windowLenMs / numBuckets;
			if (this.bucketSize % bucketSize != 0)
			{
				throw new ArgumentException("The bucket size in the rolling window is not integer: windowLenMs= "
					 + windowLenMs + " numBuckets= " + numBuckets);
			}
		}

		/// <summary>
		/// When an event occurs at the specified time, this method reflects that in
		/// the rolling window.
		/// </summary>
		/// <remarks>
		/// When an event occurs at the specified time, this method reflects that in
		/// the rolling window.
		/// <p/>
		/// </remarks>
		/// <param name="time">the time at which the event occurred</param>
		/// <param name="delta">the delta that will be added to the window</param>
		public virtual void IncAt(long time, long delta)
		{
			int bi = ComputeBucketIndex(time);
			RollingWindow.Bucket bucket = buckets[bi];
			// If the last time the bucket was updated is out of the scope of the
			// rolling window, reset the bucket.
			if (bucket.IsStaleNow(time))
			{
				bucket.SafeReset(time);
			}
			bucket.Inc(delta);
		}

		private int ComputeBucketIndex(long time)
		{
			int positionOnWindow = (int)(time % windowLenMs);
			int bucketIndex = positionOnWindow * buckets.Length / windowLenMs;
			return bucketIndex;
		}

		/// <summary>
		/// Thread-safety is provided by synchronization when resetting the update time
		/// as well as atomic fields.
		/// </summary>
		private class Bucket
		{
			internal AtomicLong value = new AtomicLong(0);

			internal AtomicLong updateTime = new AtomicLong(0);

			/// <summary>
			/// Check whether the last time that the bucket was updated is no longer
			/// covered by rolling window.
			/// </summary>
			/// <param name="time">the current time</param>
			/// <returns>true if the bucket state is stale</returns>
			internal virtual bool IsStaleNow(long time)
			{
				long utime = this.updateTime.Get();
				return time - utime >= this._enclosing.windowLenMs;
			}

			/// <summary>
			/// Safely reset the bucket state considering concurrent updates (inc) and
			/// resets.
			/// </summary>
			/// <param name="time">the current time</param>
			internal virtual void SafeReset(long time)
			{
				// At any point in time, only one thread is allowed to reset the
				// bucket
				lock (this)
				{
					if (this.IsStaleNow(time))
					{
						// reset the value before setting the time, it allows other
						// threads to safely assume that the value is updated if the
						// time is not stale
						this.value.Set(0);
						this.updateTime.Set(time);
					}
				}
			}

			// else a concurrent thread has already reset it: do nothing
			/// <summary>Increment the bucket.</summary>
			/// <remarks>
			/// Increment the bucket. It assumes that staleness check is already
			/// performed. We do not need to update the
			/// <see cref="updateTime"/>
			/// because as
			/// long as the
			/// <see cref="updateTime"/>
			/// belongs to the current view of the
			/// rolling window, the algorithm works fine.
			/// </remarks>
			internal virtual void Inc(long delta)
			{
				this.value.AddAndGet(delta);
			}

			internal Bucket(RollingWindow _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RollingWindow _enclosing;
		}

		/// <summary>
		/// Get value represented by this window at the specified time
		/// <p/>
		/// If time lags behind the latest update time, the new updates are still
		/// included in the sum
		/// </summary>
		/// <param name="time"/>
		/// <returns>number of events occurred in the past period</returns>
		public virtual long GetSum(long time)
		{
			long sum = 0;
			foreach (RollingWindow.Bucket bucket in buckets)
			{
				bool stale = bucket.IsStaleNow(time);
				if (!stale)
				{
					sum += bucket.value.Get();
				}
				if (Log.IsDebugEnabled())
				{
					long bucketTime = bucket.updateTime.Get();
					string timeStr = Sharpen.Extensions.CreateDate(bucketTime).ToString();
					Log.Debug("Sum: + " + sum + " Bucket: updateTime: " + timeStr + " (" + bucketTime
						 + ") isStale " + stale + " at " + time);
				}
			}
			return sum;
		}
	}
}
