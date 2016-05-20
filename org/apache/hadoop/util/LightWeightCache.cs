using Sharpen;

namespace org.apache.hadoop.util
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
	public class LightWeightCache<K, E> : org.apache.hadoop.util.LightWeightGSet<K, E
		>
		where E : K
	{
		/// <summary>Limit the number of entries in each eviction.</summary>
		private const int EVICTION_LIMIT = 1 << 16;

		/// <summary>
		/// Entries of
		/// <see cref="LightWeightCache{K, E}"/>
		/// .
		/// </summary>
		public interface Entry : org.apache.hadoop.util.LightWeightGSet.LinkedElement
		{
			/// <summary>Set the expiration time.</summary>
			void setExpirationTime(long timeNano);

			/// <summary>Get the expiration time.</summary>
			long getExpirationTime();
		}

		private sealed class _Comparator_71 : java.util.Comparator<org.apache.hadoop.util.LightWeightCache.Entry
			>
		{
			public _Comparator_71()
			{
			}

			public int compare(org.apache.hadoop.util.LightWeightCache.Entry left, org.apache.hadoop.util.LightWeightCache.Entry
				 right)
			{
				long l = left.getExpirationTime();
				long r = right.getExpirationTime();
				return l > r ? 1 : l < r ? -1 : 0;
			}
		}

		/// <summary>Comparator for sorting entries by expiration time in ascending order.</summary>
		private static readonly java.util.Comparator<org.apache.hadoop.util.LightWeightCache.Entry
			> expirationTimeComparator = new _Comparator_71();

		/// <summary>A clock for measuring time so that it can be mocked in unit tests.</summary>
		internal class Clock
		{
			/// <returns>the current time.</returns>
			internal virtual long currentTime()
			{
				return Sharpen.Runtime.nanoTime();
			}
		}

		private static int updateRecommendedLength(int recommendedLength, int sizeLimit)
		{
			return sizeLimit > 0 && sizeLimit < recommendedLength ? (sizeLimit / 4 * 3) : recommendedLength;
		}

		private readonly java.util.PriorityQueue<org.apache.hadoop.util.LightWeightCache.Entry
			> queue;

		private readonly long creationExpirationPeriod;

		private readonly long accessExpirationPeriod;

		private readonly int sizeLimit;

		private readonly org.apache.hadoop.util.LightWeightCache.Clock clock;

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
				, new org.apache.hadoop.util.LightWeightCache.Clock())
		{
		}

		[com.google.common.annotations.VisibleForTesting]
		internal LightWeightCache(int recommendedLength, int sizeLimit, long creationExpirationPeriod
			, long accessExpirationPeriod, org.apache.hadoop.util.LightWeightCache.Clock clock
			)
			: base(updateRecommendedLength(recommendedLength, sizeLimit))
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
				throw new System.ArgumentException("creationExpirationPeriod = " + creationExpirationPeriod
					 + " <= 0");
			}
			this.creationExpirationPeriod = creationExpirationPeriod;
			if (accessExpirationPeriod < 0)
			{
				throw new System.ArgumentException("accessExpirationPeriod = " + accessExpirationPeriod
					 + " < 0");
			}
			this.accessExpirationPeriod = accessExpirationPeriod;
			this.queue = new java.util.PriorityQueue<org.apache.hadoop.util.LightWeightCache.Entry
				>(sizeLimit > 0 ? sizeLimit + 1 : 1 << 10, expirationTimeComparator);
			this.clock = clock;
		}

		internal virtual void setExpirationTime(org.apache.hadoop.util.LightWeightCache.Entry
			 e, long expirationPeriod)
		{
			e.setExpirationTime(clock.currentTime() + expirationPeriod);
		}

		internal virtual bool isExpired(org.apache.hadoop.util.LightWeightCache.Entry e, 
			long now)
		{
			return now > e.getExpirationTime();
		}

		private E evict()
		{
			E polled = (E)queue.poll();
			E removed = base.remove(polled);
			com.google.common.@base.Preconditions.checkState(removed == polled);
			return polled;
		}

		/// <summary>Evict expired entries.</summary>
		private void evictExpiredEntries()
		{
			long now = clock.currentTime();
			for (int i = 0; i < EVICTION_LIMIT; i++)
			{
				org.apache.hadoop.util.LightWeightCache.Entry peeked = queue.peek();
				if (peeked == null || !isExpired(peeked, now))
				{
					return;
				}
				E evicted = evict();
				com.google.common.@base.Preconditions.checkState(evicted == peeked);
			}
		}

		/// <summary>Evict entries in order to enforce the size limit of the cache.</summary>
		private void evictEntries()
		{
			if (sizeLimit > 0)
			{
				for (int i = size(); i > sizeLimit; i--)
				{
					evict();
				}
			}
		}

		public override E get(K key)
		{
			E entry = base.get(key);
			if (entry != null)
			{
				if (accessExpirationPeriod > 0)
				{
					// update expiration time
					org.apache.hadoop.util.LightWeightCache.Entry existing = (org.apache.hadoop.util.LightWeightCache.Entry
						)entry;
					com.google.common.@base.Preconditions.checkState(queue.remove(existing));
					setExpirationTime(existing, accessExpirationPeriod);
					queue.offer(existing);
				}
			}
			return entry;
		}

		public override E put(E entry)
		{
			if (!(entry is org.apache.hadoop.util.LightWeightCache.Entry))
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("!(entry instanceof Entry), entry.getClass()="
					 + Sharpen.Runtime.getClassForObject(entry));
			}
			evictExpiredEntries();
			E existing = base.put(entry);
			if (existing != null)
			{
				queue.remove(existing);
			}
			org.apache.hadoop.util.LightWeightCache.Entry e = (org.apache.hadoop.util.LightWeightCache.Entry
				)entry;
			setExpirationTime(e, creationExpirationPeriod);
			queue.offer(e);
			evictEntries();
			return existing;
		}

		public override E remove(K key)
		{
			evictExpiredEntries();
			E removed = base.remove(key);
			if (removed != null)
			{
				com.google.common.@base.Preconditions.checkState(queue.remove(removed));
			}
			return removed;
		}

		public override System.Collections.Generic.IEnumerator<E> GetEnumerator()
		{
			System.Collections.Generic.IEnumerator<E> iter = base.GetEnumerator();
			return new _IEnumerator_243(iter);
		}

		private sealed class _IEnumerator_243 : System.Collections.Generic.IEnumerator<E>
		{
			public _IEnumerator_243(System.Collections.Generic.IEnumerator<E> iter)
			{
				this.iter = iter;
			}

			public override bool MoveNext()
			{
				return iter.MoveNext();
			}

			public override E Current
			{
				get
				{
					return iter.Current;
				}
			}

			public override void remove()
			{
				// It would be tricky to support this because LightWeightCache#remove
				// may evict multiple elements via evictExpiredEntries.
				throw new System.NotSupportedException("Remove via iterator is " + "not supported for LightWeightCache"
					);
			}

			private readonly System.Collections.Generic.IEnumerator<E> iter;
		}
	}
}
