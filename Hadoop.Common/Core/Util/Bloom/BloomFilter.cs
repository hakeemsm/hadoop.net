using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Util.Bloom
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
	public class BloomFilter : Filter
	{
		private static readonly byte[] bitvalues = new byte[] { unchecked((byte)unchecked(
			(int)(0x01))), unchecked((byte)unchecked((int)(0x02))), unchecked((byte)unchecked(
			(int)(0x04))), unchecked((byte)unchecked((int)(0x08))), unchecked((byte)unchecked(
			(int)(0x10))), unchecked((byte)unchecked((int)(0x20))), unchecked((byte)unchecked(
			(int)(0x40))), unchecked((byte)unchecked((int)(0x80))) };

		/// <summary>The bit vector.</summary>
		internal BitSet bits;

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
		/// <see cref="Org.Apache.Hadoop.Util.Hash.Hash"/>
		/// ).
		/// </param>
		public BloomFilter(int vectorSize, int nbHash, int hashType)
			: base(vectorSize, nbHash, hashType)
		{
			bits = new BitSet(this.vectorSize);
		}

		public override void Add(Key key)
		{
			if (key == null)
			{
				throw new ArgumentNullException("key cannot be null");
			}
			int[] h = hash.Hash(key);
			hash.Clear();
			for (int i = 0; i < nbHash; i++)
			{
				bits.Set(h[i]);
			}
		}

		public override void And(Filter filter)
		{
			if (filter == null || !(filter is Org.Apache.Hadoop.Util.Bloom.BloomFilter) || filter
				.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new ArgumentException("filters cannot be and-ed");
			}
			this.bits.And(((Org.Apache.Hadoop.Util.Bloom.BloomFilter)filter).bits);
		}

		public override bool MembershipTest(Key key)
		{
			if (key == null)
			{
				throw new ArgumentNullException("key cannot be null");
			}
			int[] h = hash.Hash(key);
			hash.Clear();
			for (int i = 0; i < nbHash; i++)
			{
				if (!bits.Get(h[i]))
				{
					return false;
				}
			}
			return true;
		}

		public override void Not()
		{
			bits.Flip(0, vectorSize);
		}

		public override void Or(Filter filter)
		{
			if (filter == null || !(filter is Org.Apache.Hadoop.Util.Bloom.BloomFilter) || filter
				.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new ArgumentException("filters cannot be or-ed");
			}
			bits.Or(((Org.Apache.Hadoop.Util.Bloom.BloomFilter)filter).bits);
		}

		public override void Xor(Filter filter)
		{
			if (filter == null || !(filter is Org.Apache.Hadoop.Util.Bloom.BloomFilter) || filter
				.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new ArgumentException("filters cannot be xor-ed");
			}
			bits.Xor(((Org.Apache.Hadoop.Util.Bloom.BloomFilter)filter).bits);
		}

		public override string ToString()
		{
			return bits.ToString();
		}

		/// <returns>size of the the bloomfilter</returns>
		public virtual int GetVectorSize()
		{
			return this.vectorSize;
		}

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public override void Write(BinaryWriter @out)
		{
			base.Write(@out);
			byte[] bytes = new byte[GetNBytes()];
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
				if (bits.Get(i))
				{
					bytes[byteIndex] |= bitvalues[bitIndex];
				}
			}
			@out.Write(bytes);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(BinaryReader @in)
		{
			base.ReadFields(@in);
			bits = new BitSet(this.vectorSize);
			byte[] bytes = new byte[GetNBytes()];
			@in.ReadFully(bytes);
			for (int i = 0; i < vectorSize; i++, bitIndex++)
			{
				if (bitIndex == 8)
				{
					bitIndex = 0;
					byteIndex++;
				}
				if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0)
				{
					bits.Set(i);
				}
			}
		}

		/* @return number of bytes needed to hold bit vector */
		private int GetNBytes()
		{
			return (vectorSize + 7) / 8;
		}
		//end class
	}
}
