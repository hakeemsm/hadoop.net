using Sharpen;

namespace org.apache.hadoop.util.bloom
{
	/// <summary>Implements a <i>counting Bloom filter</i>, as defined by Fan et al.</summary>
	/// <remarks>
	/// Implements a <i>counting Bloom filter</i>, as defined by Fan et al. in a ToN
	/// 2000 paper.
	/// <p>
	/// A counting Bloom filter is an improvement to standard a Bloom filter as it
	/// allows dynamic additions and deletions of set membership information.  This
	/// is achieved through the use of a counting vector instead of a bit vector.
	/// <p>
	/// Originally created by
	/// <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
	/// </remarks>
	/// <seealso cref="Filter">The general behavior of a filter</seealso>
	/// <seealso><a href="http://portal.acm.org/citation.cfm?id=343571.343572">Summary cache: a scalable wide-area web cache sharing protocol</a>
	/// 	</seealso>
	public sealed class CountingBloomFilter : org.apache.hadoop.util.bloom.Filter
	{
		/// <summary>Storage for the counting buckets</summary>
		private long[] buckets;

		/// <summary>We are using 4bit buckets, so each bucket can count to 15</summary>
		private const long BUCKET_MAX_VALUE = 15;

		/// <summary>Default constructor - use with readFields</summary>
		public CountingBloomFilter()
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="vectorSize">The vector size of <i>this</i> filter.</param>
		/// <param name="nbHash">The number of hash function to consider.</param>
		/// <param name="hashType">
		/// type of the hashing function (see
		/// <see cref="org.apache.hadoop.util.hash.Hash"/>
		/// ).
		/// </param>
		public CountingBloomFilter(int vectorSize, int nbHash, int hashType)
			: base(vectorSize, nbHash, hashType)
		{
			buckets = new long[buckets2words(vectorSize)];
		}

		/// <summary>returns the number of 64 bit words it would take to hold vectorSize buckets
		/// 	</summary>
		private static int buckets2words(int vectorSize)
		{
			return ((int)(((uint)(vectorSize - 1)) >> 4)) + 1;
		}

		public override void add(org.apache.hadoop.util.bloom.Key key)
		{
			if (key == null)
			{
				throw new System.ArgumentNullException("key can not be null");
			}
			int[] h = hash.hash(key);
			hash.clear();
			for (int i = 0; i < nbHash; i++)
			{
				// find the bucket
				int wordNum = h[i] >> 4;
				// div 16
				int bucketShift = (h[i] & unchecked((int)(0x0f))) << 2;
				// (mod 16) * 4
				long bucketMask = 15L << bucketShift;
				long bucketValue = (long)(((ulong)(buckets[wordNum] & bucketMask)) >> bucketShift
					);
				// only increment if the count in the bucket is less than BUCKET_MAX_VALUE
				if (bucketValue < BUCKET_MAX_VALUE)
				{
					// increment by 1
					buckets[wordNum] = (buckets[wordNum] & ~bucketMask) | ((bucketValue + 1) << bucketShift
						);
				}
			}
		}

		/// <summary>Removes a specified key from <i>this</i> counting Bloom filter.</summary>
		/// <remarks>
		/// Removes a specified key from <i>this</i> counting Bloom filter.
		/// <p>
		/// <b>Invariant</b>: nothing happens if the specified key does not belong to <i>this</i> counter Bloom filter.
		/// </remarks>
		/// <param name="key">The key to remove.</param>
		public void delete(org.apache.hadoop.util.bloom.Key key)
		{
			if (key == null)
			{
				throw new System.ArgumentNullException("Key may not be null");
			}
			if (!membershipTest(key))
			{
				throw new System.ArgumentException("Key is not a member");
			}
			int[] h = hash.hash(key);
			hash.clear();
			for (int i = 0; i < nbHash; i++)
			{
				// find the bucket
				int wordNum = h[i] >> 4;
				// div 16
				int bucketShift = (h[i] & unchecked((int)(0x0f))) << 2;
				// (mod 16) * 4
				long bucketMask = 15L << bucketShift;
				long bucketValue = (long)(((ulong)(buckets[wordNum] & bucketMask)) >> bucketShift
					);
				// only decrement if the count in the bucket is between 0 and BUCKET_MAX_VALUE
				if (bucketValue >= 1 && bucketValue < BUCKET_MAX_VALUE)
				{
					// decrement by 1
					buckets[wordNum] = (buckets[wordNum] & ~bucketMask) | ((bucketValue - 1) << bucketShift
						);
				}
			}
		}

		public override void and(org.apache.hadoop.util.bloom.Filter filter)
		{
			if (filter == null || !(filter is org.apache.hadoop.util.bloom.CountingBloomFilter
				) || filter.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new System.ArgumentException("filters cannot be and-ed");
			}
			org.apache.hadoop.util.bloom.CountingBloomFilter cbf = (org.apache.hadoop.util.bloom.CountingBloomFilter
				)filter;
			int sizeInWords = buckets2words(vectorSize);
			for (int i = 0; i < sizeInWords; i++)
			{
				this.buckets[i] &= cbf.buckets[i];
			}
		}

		public override bool membershipTest(org.apache.hadoop.util.bloom.Key key)
		{
			if (key == null)
			{
				throw new System.ArgumentNullException("Key may not be null");
			}
			int[] h = hash.hash(key);
			hash.clear();
			for (int i = 0; i < nbHash; i++)
			{
				// find the bucket
				int wordNum = h[i] >> 4;
				// div 16
				int bucketShift = (h[i] & unchecked((int)(0x0f))) << 2;
				// (mod 16) * 4
				long bucketMask = 15L << bucketShift;
				if ((buckets[wordNum] & bucketMask) == 0)
				{
					return false;
				}
			}
			return true;
		}

		/// <summary>This method calculates an approximate count of the key, i.e.</summary>
		/// <remarks>
		/// This method calculates an approximate count of the key, i.e. how many
		/// times the key was added to the filter. This allows the filter to be
		/// used as an approximate <code>key -&gt; count</code> map.
		/// <p>NOTE: due to the bucket size of this filter, inserting the same
		/// key more than 15 times will cause an overflow at all filter positions
		/// associated with this key, and it will significantly increase the error
		/// rate for this and other keys. For this reason the filter can only be
		/// used to store small count values <code>0 &lt;= N &lt;&lt; 15</code>.
		/// </remarks>
		/// <param name="key">key to be tested</param>
		/// <returns>
		/// 0 if the key is not present. Otherwise, a positive value v will
		/// be returned such that <code>v == count</code> with probability equal to the
		/// error rate of this filter, and <code>v &gt; count</code> otherwise.
		/// Additionally, if the filter experienced an underflow as a result of
		/// <see cref="delete(Key)"/>
		/// operation, the return value may be lower than the
		/// <code>count</code> with the probability of the false negative rate of such
		/// filter.
		/// </returns>
		public int approximateCount(org.apache.hadoop.util.bloom.Key key)
		{
			int res = int.MaxValue;
			int[] h = hash.hash(key);
			hash.clear();
			for (int i = 0; i < nbHash; i++)
			{
				// find the bucket
				int wordNum = h[i] >> 4;
				// div 16
				int bucketShift = (h[i] & unchecked((int)(0x0f))) << 2;
				// (mod 16) * 4
				long bucketMask = 15L << bucketShift;
				long bucketValue = (long)(((ulong)(buckets[wordNum] & bucketMask)) >> bucketShift
					);
				if (bucketValue < res)
				{
					res = (int)bucketValue;
				}
			}
			if (res != int.MaxValue)
			{
				return res;
			}
			else
			{
				return 0;
			}
		}

		public override void not()
		{
			throw new System.NotSupportedException("not() is undefined for " + Sharpen.Runtime.getClassForObject
				(this).getName());
		}

		public override void or(org.apache.hadoop.util.bloom.Filter filter)
		{
			if (filter == null || !(filter is org.apache.hadoop.util.bloom.CountingBloomFilter
				) || filter.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new System.ArgumentException("filters cannot be or-ed");
			}
			org.apache.hadoop.util.bloom.CountingBloomFilter cbf = (org.apache.hadoop.util.bloom.CountingBloomFilter
				)filter;
			int sizeInWords = buckets2words(vectorSize);
			for (int i = 0; i < sizeInWords; i++)
			{
				this.buckets[i] |= cbf.buckets[i];
			}
		}

		public override void xor(org.apache.hadoop.util.bloom.Filter filter)
		{
			throw new System.NotSupportedException("xor() is undefined for " + Sharpen.Runtime.getClassForObject
				(this).getName());
		}

		public override string ToString()
		{
			java.lang.StringBuilder res = new java.lang.StringBuilder();
			for (int i = 0; i < vectorSize; i++)
			{
				if (i > 0)
				{
					res.Append(" ");
				}
				int wordNum = i >> 4;
				// div 16
				int bucketShift = (i & unchecked((int)(0x0f))) << 2;
				// (mod 16) * 4
				long bucketMask = 15L << bucketShift;
				long bucketValue = (long)(((ulong)(buckets[wordNum] & bucketMask)) >> bucketShift
					);
				res.Append(bucketValue);
			}
			return res.ToString();
		}

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public override void write(java.io.DataOutput @out)
		{
			base.write(@out);
			int sizeInWords = buckets2words(vectorSize);
			for (int i = 0; i < sizeInWords; i++)
			{
				@out.writeLong(buckets[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void readFields(java.io.DataInput @in)
		{
			base.readFields(@in);
			int sizeInWords = buckets2words(vectorSize);
			buckets = new long[sizeInWords];
			for (int i = 0; i < sizeInWords; i++)
			{
				buckets[i] = @in.readLong();
			}
		}
	}
}
