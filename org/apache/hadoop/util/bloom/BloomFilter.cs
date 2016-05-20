using Sharpen;

namespace org.apache.hadoop.util.bloom
{
	/// <summary>Implements a <i>Bloom filter</i>, as defined by Bloom in 1970.</summary>
	/// <remarks>
	/// Implements a <i>Bloom filter</i>, as defined by Bloom in 1970.
	/// <p>
	/// The Bloom filter is a data structure that was introduced in 1970 and that has been adopted by
	/// the networking research community in the past decade thanks to the bandwidth efficiencies that it
	/// offers for the transmission of set membership information between networked hosts.  A sender encodes
	/// the information into a bit vector, the Bloom filter, that is more compact than a conventional
	/// representation. Computation and space costs for construction are linear in the number of elements.
	/// The receiver uses the filter to test whether various elements are members of the set. Though the
	/// filter will occasionally return a false positive, it will never return a false negative. When creating
	/// the filter, the sender can choose its desired point in a trade-off between the false positive rate and the size.
	/// <p>
	/// Originally created by
	/// <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
	/// </remarks>
	/// <seealso cref="Filter">The general behavior of a filter</seealso>
	/// <seealso><a href="http://portal.acm.org/citation.cfm?id=362692&dl=ACM&coll=portal">Space/Time Trade-Offs in Hash Coding with Allowable Errors</a>
	/// 	</seealso>
	public class BloomFilter : org.apache.hadoop.util.bloom.Filter
	{
		private static readonly byte[] bitvalues = new byte[] { unchecked((byte)unchecked(
			(int)(0x01))), unchecked((byte)unchecked((int)(0x02))), unchecked((byte)unchecked(
			(int)(0x04))), unchecked((byte)unchecked((int)(0x08))), unchecked((byte)unchecked(
			(int)(0x10))), unchecked((byte)unchecked((int)(0x20))), unchecked((byte)unchecked(
			(int)(0x40))), unchecked((byte)unchecked((int)(0x80))) };

		/// <summary>The bit vector.</summary>
		internal java.util.BitSet bits;

		/// <summary>Default constructor - use with readFields</summary>
		public BloomFilter()
			: base()
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
		public BloomFilter(int vectorSize, int nbHash, int hashType)
			: base(vectorSize, nbHash, hashType)
		{
			bits = new java.util.BitSet(this.vectorSize);
		}

		public override void add(org.apache.hadoop.util.bloom.Key key)
		{
			if (key == null)
			{
				throw new System.ArgumentNullException("key cannot be null");
			}
			int[] h = hash.hash(key);
			hash.clear();
			for (int i = 0; i < nbHash; i++)
			{
				bits.set(h[i]);
			}
		}

		public override void and(org.apache.hadoop.util.bloom.Filter filter)
		{
			if (filter == null || !(filter is org.apache.hadoop.util.bloom.BloomFilter) || filter
				.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new System.ArgumentException("filters cannot be and-ed");
			}
			this.bits.and(((org.apache.hadoop.util.bloom.BloomFilter)filter).bits);
		}

		public override bool membershipTest(org.apache.hadoop.util.bloom.Key key)
		{
			if (key == null)
			{
				throw new System.ArgumentNullException("key cannot be null");
			}
			int[] h = hash.hash(key);
			hash.clear();
			for (int i = 0; i < nbHash; i++)
			{
				if (!bits.get(h[i]))
				{
					return false;
				}
			}
			return true;
		}

		public override void not()
		{
			bits.flip(0, vectorSize);
		}

		public override void or(org.apache.hadoop.util.bloom.Filter filter)
		{
			if (filter == null || !(filter is org.apache.hadoop.util.bloom.BloomFilter) || filter
				.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new System.ArgumentException("filters cannot be or-ed");
			}
			bits.or(((org.apache.hadoop.util.bloom.BloomFilter)filter).bits);
		}

		public override void xor(org.apache.hadoop.util.bloom.Filter filter)
		{
			if (filter == null || !(filter is org.apache.hadoop.util.bloom.BloomFilter) || filter
				.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new System.ArgumentException("filters cannot be xor-ed");
			}
			bits.xor(((org.apache.hadoop.util.bloom.BloomFilter)filter).bits);
		}

		public override string ToString()
		{
			return bits.ToString();
		}

		/// <returns>size of the the bloomfilter</returns>
		public virtual int getVectorSize()
		{
			return this.vectorSize;
		}

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public override void write(java.io.DataOutput @out)
		{
			base.write(@out);
			byte[] bytes = new byte[getNBytes()];
			for (int i = 0; i < vectorSize; i++, bitIndex++)
			{
				if (bitIndex == 8)
				{
					bitIndex = 0;
					byteIndex++;
				}
				if (bitIndex == 0)
				{
					bytes[byteIndex] = 0;
				}
				if (bits.get(i))
				{
					bytes[byteIndex] |= bitvalues[bitIndex];
				}
			}
			@out.write(bytes);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void readFields(java.io.DataInput @in)
		{
			base.readFields(@in);
			bits = new java.util.BitSet(this.vectorSize);
			byte[] bytes = new byte[getNBytes()];
			@in.readFully(bytes);
			for (int i = 0; i < vectorSize; i++, bitIndex++)
			{
				if (bitIndex == 8)
				{
					bitIndex = 0;
					byteIndex++;
				}
				if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0)
				{
					bits.set(i);
				}
			}
		}

		/* @return number of bytes needed to hold bit vector */
		private int getNBytes()
		{
			return (vectorSize + 7) / 8;
		}
		//end class
	}
}
