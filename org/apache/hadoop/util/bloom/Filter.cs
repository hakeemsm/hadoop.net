using Sharpen;

namespace org.apache.hadoop.util.bloom
{
	/// <summary>Defines the general behavior of a filter.</summary>
	/// <remarks>
	/// Defines the general behavior of a filter.
	/// <p>
	/// A filter is a data structure which aims at offering a lossy summary of a set <code>A</code>.  The
	/// key idea is to map entries of <code>A</code> (also called <i>keys</i>) into several positions
	/// in a vector through the use of several hash functions.
	/// <p>
	/// Typically, a filter will be implemented as a Bloom filter (or a Bloom filter extension).
	/// <p>
	/// It must be extended in order to define the real behavior.
	/// </remarks>
	/// <seealso cref="Key">The general behavior of a key</seealso>
	/// <seealso cref="HashFunction">A hash function</seealso>
	public abstract class Filter : org.apache.hadoop.io.Writable
	{
		private const int VERSION = -1;

		/// <summary>The vector size of <i>this</i> filter.</summary>
		protected internal int vectorSize;

		/// <summary>The hash function used to map a key to several positions in the vector.</summary>
		protected internal org.apache.hadoop.util.bloom.HashFunction hash;

		/// <summary>The number of hash function to consider.</summary>
		protected internal int nbHash;

		/// <summary>Type of hashing function to use.</summary>
		protected internal int hashType;

		protected internal Filter()
		{
		}

		/// <summary>Constructor.</summary>
		/// <param name="vectorSize">The vector size of <i>this</i> filter.</param>
		/// <param name="nbHash">The number of hash functions to consider.</param>
		/// <param name="hashType">
		/// type of the hashing function (see
		/// <see cref="org.apache.hadoop.util.hash.Hash"/>
		/// ).
		/// </param>
		protected internal Filter(int vectorSize, int nbHash, int hashType)
		{
			// negative to accommodate for old format 
			this.vectorSize = vectorSize;
			this.nbHash = nbHash;
			this.hashType = hashType;
			this.hash = new org.apache.hadoop.util.bloom.HashFunction(this.vectorSize, this.nbHash
				, this.hashType);
		}

		/// <summary>Adds a key to <i>this</i> filter.</summary>
		/// <param name="key">The key to add.</param>
		public abstract void add(org.apache.hadoop.util.bloom.Key key);

		/// <summary>Determines wether a specified key belongs to <i>this</i> filter.</summary>
		/// <param name="key">The key to test.</param>
		/// <returns>
		/// boolean True if the specified key belongs to <i>this</i> filter.
		/// False otherwise.
		/// </returns>
		public abstract bool membershipTest(org.apache.hadoop.util.bloom.Key key);

		/// <summary>Peforms a logical AND between <i>this</i> filter and a specified filter.
		/// 	</summary>
		/// <remarks>
		/// Peforms a logical AND between <i>this</i> filter and a specified filter.
		/// <p>
		/// <b>Invariant</b>: The result is assigned to <i>this</i> filter.
		/// </remarks>
		/// <param name="filter">The filter to AND with.</param>
		public abstract void and(org.apache.hadoop.util.bloom.Filter filter);

		/// <summary>Peforms a logical OR between <i>this</i> filter and a specified filter.</summary>
		/// <remarks>
		/// Peforms a logical OR between <i>this</i> filter and a specified filter.
		/// <p>
		/// <b>Invariant</b>: The result is assigned to <i>this</i> filter.
		/// </remarks>
		/// <param name="filter">The filter to OR with.</param>
		public abstract void or(org.apache.hadoop.util.bloom.Filter filter);

		/// <summary>Peforms a logical XOR between <i>this</i> filter and a specified filter.
		/// 	</summary>
		/// <remarks>
		/// Peforms a logical XOR between <i>this</i> filter and a specified filter.
		/// <p>
		/// <b>Invariant</b>: The result is assigned to <i>this</i> filter.
		/// </remarks>
		/// <param name="filter">The filter to XOR with.</param>
		public abstract void xor(org.apache.hadoop.util.bloom.Filter filter);

		/// <summary>Performs a logical NOT on <i>this</i> filter.</summary>
		/// <remarks>
		/// Performs a logical NOT on <i>this</i> filter.
		/// <p>
		/// The result is assigned to <i>this</i> filter.
		/// </remarks>
		public abstract void not();

		/// <summary>Adds a list of keys to <i>this</i> filter.</summary>
		/// <param name="keys">The list of keys.</param>
		public virtual void add(System.Collections.Generic.IList<org.apache.hadoop.util.bloom.Key
			> keys)
		{
			if (keys == null)
			{
				throw new System.ArgumentException("ArrayList<Key> may not be null");
			}
			foreach (org.apache.hadoop.util.bloom.Key key in keys)
			{
				add(key);
			}
		}

		//end add()
		/// <summary>Adds a collection of keys to <i>this</i> filter.</summary>
		/// <param name="keys">The collection of keys.</param>
		public virtual void add(System.Collections.Generic.ICollection<org.apache.hadoop.util.bloom.Key
			> keys)
		{
			if (keys == null)
			{
				throw new System.ArgumentException("Collection<Key> may not be null");
			}
			foreach (org.apache.hadoop.util.bloom.Key key in keys)
			{
				add(key);
			}
		}

		//end add()
		/// <summary>Adds an array of keys to <i>this</i> filter.</summary>
		/// <param name="keys">The array of keys.</param>
		public virtual void add(org.apache.hadoop.util.bloom.Key[] keys)
		{
			if (keys == null)
			{
				throw new System.ArgumentException("Key[] may not be null");
			}
			for (int i = 0; i < keys.Length; i++)
			{
				add(keys[i]);
			}
		}

		//end add()
		// Writable interface
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeInt(VERSION);
			@out.writeInt(this.nbHash);
			@out.writeByte(this.hashType);
			@out.writeInt(this.vectorSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			int ver = @in.readInt();
			if (ver > 0)
			{
				// old unversioned format
				this.nbHash = ver;
				this.hashType = org.apache.hadoop.util.hash.Hash.JENKINS_HASH;
			}
			else
			{
				if (ver == VERSION)
				{
					this.nbHash = @in.readInt();
					this.hashType = @in.readByte();
				}
				else
				{
					throw new System.IO.IOException("Unsupported version: " + ver);
				}
			}
			this.vectorSize = @in.readInt();
			this.hash = new org.apache.hadoop.util.bloom.HashFunction(this.vectorSize, this.nbHash
				, this.hashType);
		}
		//end class
	}
}
