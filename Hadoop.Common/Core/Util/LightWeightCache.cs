using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Hadoop;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// A low memory footprint Cache which extends
	/// <see cref="LightWeightGSet{K, E}"/>
	/// .
	/// An entry in the cache is expired if
	/// (1) it is added to the cache longer than the creation-expiration period, and
	/// (2) it is not accessed for the access-expiration period.
	/// When an entry is expired, it may be evicted from the cache.
	/// When the size limit of the cache is set, the cache will evict the entries
	/// with earliest expiration time, even if they are not expired.
	/// It is guaranteed that number of entries in the cache is less than or equal
	/// to the size limit.  However, It is not guaranteed that expired entries are
	/// evicted from the cache. An expired entry may possibly be accessed after its
	/// expiration time. In such case, the expiration time may be updated.
	/// This class does not support null entry.
	/// This class is not thread safe.
	/// </summary>
	/// <?/>
	/// <?/>
	public class LightWeightCache<K, E> : LightWeightGSet<K, E>
		where E : K
	{
		/// <summary>Limit the number of entries in each eviction.</summary>
		private const int EvictionLimit = 1 << 16;

		/// <summary>
		/// Entries of
		/// <see cref="LightWeightCache{K, E}"/>
		/// .
		/// </summary>
		public interface Entry : LightWeightGSet.LinkedElement
		{
			/// <summary>Set the expiration time.</summary>
			void SetExpirationTime(long timeNano);

			/// <summary>Get the expiration time.</summary>
			long GetExpirationTime();
		}

		private sealed class _IComparer_71 : IComparer<LightWeightCache.Entry>
		{
			public _IComparer_71()
			{
			}

			public int Compare(LightWeightCache.Entry left, LightWeightCache.Entry right)
			{
				long l = left.GetExpirationTime();
				long r = right.GetExpirationTime();
				return l > r ? 1 : l < r ? -1 : 0;
			}
		}

		/// <summary>Comparator for sorting entries by expiration time in ascending order.</summary>
		private static readonly IComparer<LightWeightCache.Entry> expirationTimeComparator
			 = new _IComparer_71();

		/// <summary>A clock for measuring time so that it can be mocked in unit tests.</summary>
		internal class Clock
		{
			/// <returns>the current time.</returns>
			internal virtual long CurrentTime()
			{
				return Runtime.NanoTime();
			}
		}

		private static int UpdateRecommendedLength(int recommendedLength, int sizeLimit)
		{
			return sizeLimit > 0 && sizeLimit < recommendedLength ? (sizeLimit / 4 * 3) : recommendedLength;
		}

		private readonly PriorityQueue<LightWeightCache.Entry> queue;

		private readonly long creationExpirationPeriod;

		private readonly long accessExpirationPeriod;

		private readonly int sizeLimit;

		private readonly LightWeightCache.Clock clock;

		/// <param name="recommendedLength">Recommended size of the internal array.</param>
		/// <param name="sizeLimit">
		/// the limit of the size of the cache.
		/// The limit is disabled if it is &lt;= 0.
		/// </param>
		/// <param name="creationExpirationPeriod">
		/// the time period C &gt; 0 in nanoseconds that
		/// the creation of an entry is expired if it is added to the cache
		/// longer than C.
		/// </param>
		/// <param name="accessExpirationPeriod">
		/// the time period A &gt;= 0 in nanoseconds that
		/// the access of an entry is expired if it is not accessed
		/// longer than A.
		/// </param>
		public LightWeightCache(int recommendedLength, int sizeLimit, long creationExpirationPeriod
			, long accessExpirationPeriod)
			: this(recommendedLength, sizeLimit, creationExpirationPeriod, accessExpirationPeriod
				, new LightWeightCache.Clock())
		{
		}

		[VisibleForTesting]
		internal LightWeightCache(int recommendedLength, int sizeLimit, long creationExpirationPeriod
			, long accessExpirationPeriod, LightWeightCache.Clock clock)
			: base(UpdateRecommendedLength(recommendedLength, sizeLimit))
		{
			// 0.75 load factor
			/*
			* The memory footprint for java.util.PriorityQueue is low but the
			* remove(Object) method runs in linear time. We may improve it by using a
			* balanced tree. However, we do not yet have a low memory footprint balanced
			* tree implementation.
			*/
			this.sizeLimit = sizeLimit;
			if (creationExpirationPeriod <= 0)
			{
				throw new ArgumentException("creationExpirationPeriod = " + creationExpirationPeriod
					 + " <= 0");
			}
			this.creationExpirationPeriod = creationExpirationPeriod;
			if (accessExpirationPeriod < 0)
			{
				throw new ArgumentException("accessExpirationPeriod = " + accessExpirationPeriod 
					+ " < 0");
			}
			this.accessExpirationPeriod = accessExpirationPeriod;
			this.queue = new PriorityQueue<LightWeightCache.Entry>(sizeLimit > 0 ? sizeLimit 
				+ 1 : 1 << 10, expirationTimeComparator);
			this.clock = clock;
		}

		internal virtual void SetExpirationTime(LightWeightCache.Entry e, long expirationPeriod
			)
		{
			e.SetExpirationTime(clock.CurrentTime() + expirationPeriod);
		}

		internal virtual bool IsExpired(LightWeightCache.Entry e, long now)
		{
			return now > e.GetExpirationTime();
		}

		private E Evict()
		{
			E polled = (E)queue.Poll();
			E removed = base.Remove(polled);
			Preconditions.CheckState(removed == polled);
			return polled;
		}

		/// <summary>Evict expired entries.</summary>
		private void EvictExpiredEntries()
		{
			long now = clock.CurrentTime();
			for (int i = 0; i < EvictionLimit; i++)
			{
				LightWeightCache.Entry peeked = queue.Peek();
				if (peeked == null || !IsExpired(peeked, now))
				{
					return;
				}
				E evicted = Evict();
				Preconditions.CheckState(evicted == peeked);
			}
		}

		/// <summary>Evict entries in order to enforce the size limit of the cache.</summary>
		private void EvictEntries()
		{
			if (sizeLimit > 0)
			{
				for (int i = Size(); i > sizeLimit; i--)
				{
					Evict();
				}
			}
		}

		public override E Get(K key)
		{
			E entry = base.Get(key);
			if (entry != null)
			{
				if (accessExpirationPeriod > 0)
				{
					// update expiration time
					LightWeightCache.Entry existing = (LightWeightCache.Entry)entry;
					Preconditions.CheckState(queue.Remove(existing));
					SetExpirationTime(existing, accessExpirationPeriod);
					queue.Offer(existing);
				}
			}
			return entry;
		}

		public override E Put(E entry)
		{
			if (!(entry is LightWeightCache.Entry))
			{
				throw new HadoopIllegalArgumentException("!(entry instanceof Entry), entry.getClass()="
					 + entry.GetType());
			}
			EvictExpiredEntries();
			E existing = base.Put(entry);
			if (existing != null)
			{
				queue.Remove(existing);
			}
			LightWeightCache.Entry e = (LightWeightCache.Entry)entry;
			SetExpirationTime(e, creationExpirationPeriod);
			queue.Offer(e);
			EvictEntries();
			return existing;
		}

		public override E Remove(K key)
		{
			EvictExpiredEntries();
			E removed = base.Remove(key);
			if (removed != null)
			{
				Preconditions.CheckState(queue.Remove(removed));
			}
			return removed;
		}

		public override IEnumerator<E> GetEnumerator()
		{
			IEnumerator<E> iter = base.GetEnumerator();
			return new _IEnumerator_243(iter);
		}

		private sealed class _IEnumerator_243 : IEnumerator<E>
		{
			public _IEnumerator_243(IEnumerator<E> iter)
			{
				this.iter = iter;
			}

			public override bool HasNext()
			{
				return iter.HasNext();
			}

			public override E Next()
			{
				return iter.Next();
			}

			public override void Remove()
			{
				// It would be tricky to support this because LightWeightCache#remove
				// may evict multiple elements via evictExpiredEntries.
				throw new NotSupportedException("Remove via iterator is " + "not supported for LightWeightCache"
					);
			}

			private readonly IEnumerator<E> iter;
		}
	}
}
