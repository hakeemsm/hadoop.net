using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.Util.Bloom
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
	public abstract class Filter : IWritable
	{
		private const int Version = -1;

		/// <summary>The vector size of <i>this</i> filter.</summary>
		protected internal int vectorSize;

		/// <summary>The hash function used to map a key to several positions in the vector.</summary>
		protected internal HashFunction hash;

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
		/// <see cref="Org.Apache.Hadoop.Util.Hash.Hash"/>
		/// ).
		/// </param>
		protected internal Filter(int vectorSize, int nbHash, int hashType)
		{
			// negative to accommodate for old format 
			this.vectorSize = vectorSize;
			this.nbHash = nbHash;
			this.hashType = hashType;
			this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
		}

		/// <summary>Adds a key to <i>this</i> filter.</summary>
		/// <param name="key">The key to add.</param>
		public abstract void Add(Key key);

		/// <summary>Determines wether a specified key belongs to <i>this</i> filter.</summary>
		/// <param name="key">The key to test.</param>
		/// <returns>
		/// boolean True if the specified key belongs to <i>this</i> filter.
		/// False otherwise.
		/// </returns>
		public abstract bool MembershipTest(Key key);

		/// <summary>Peforms a logical AND between <i>this</i> filter and a specified filter.
		/// 	</summary>
		/// <remarks>
		/// Peforms a logical AND between <i>this</i> filter and a specified filter.
		/// <p>
		/// <b>Invariant</b>: The result is assigned to <i>this</i> filter.
		/// </remarks>
		/// <param name="filter">The filter to AND with.</param>
		public abstract void And(Org.Apache.Hadoop.Util.Bloom.Filter filter);

		/// <summary>Peforms a logical OR between <i>this</i> filter and a specified filter.</summary>
		/// <remarks>
		/// Peforms a logical OR between <i>this</i> filter and a specified filter.
		/// <p>
		/// <b>Invariant</b>: The result is assigned to <i>this</i> filter.
		/// </remarks>
		/// <param name="filter">The filter to OR with.</param>
		public abstract void Or(Org.Apache.Hadoop.Util.Bloom.Filter filter);

		/// <summary>Peforms a logical XOR between <i>this</i> filter and a specified filter.
		/// 	</summary>
		/// <remarks>
		/// Peforms a logical XOR between <i>this</i> filter and a specified filter.
		/// <p>
		/// <b>Invariant</b>: The result is assigned to <i>this</i> filter.
		/// </remarks>
		/// <param name="filter">The filter to XOR with.</param>
		public abstract void Xor(Org.Apache.Hadoop.Util.Bloom.Filter filter);

		/// <summary>Performs a logical NOT on <i>this</i> filter.</summary>
		/// <remarks>
		/// Performs a logical NOT on <i>this</i> filter.
		/// <p>
		/// The result is assigned to <i>this</i> filter.
		/// </remarks>
		public abstract void Not();

		/// <summary>Adds a list of keys to <i>this</i> filter.</summary>
		/// <param name="keys">The list of keys.</param>
		public virtual void Add(IList<Key> keys)
		{
			if (keys == null)
			{
				throw new ArgumentException("ArrayList<Key> may not be null");
			}
			foreach (Key key in keys)
			{
				Add(key);
			}
		}

		//end add()
		/// <summary>Adds a collection of keys to <i>this</i> filter.</summary>
		/// <param name="keys">The collection of keys.</param>
		public virtual void Add(ICollection<Key> keys)
		{
			if (keys == null)
			{
				throw new ArgumentException("Collection<Key> may not be null");
			}
			foreach (Key key in keys)
			{
				Add(key);
			}
		}

		//end add()
		/// <summary>Adds an array of keys to <i>this</i> filter.</summary>
		/// <param name="keys">The array of keys.</param>
		public virtual void Add(Key[] keys)
		{
			if (keys == null)
			{
				throw new ArgumentException("Key[] may not be null");
			}
			for (int i = 0; i < keys.Length; i++)
			{
				Add(keys[i]);
			}
		}

		//end add()
		// Writable interface
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			@out.WriteInt(Version);
			@out.WriteInt(this.nbHash);
			@out.WriteByte(this.hashType);
			@out.WriteInt(this.vectorSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader reader)
		{
			int ver = @in.ReadInt();
			if (ver > 0)
			{
				// old unversioned format
				this.nbHash = ver;
				this.hashType = Org.Apache.Hadoop.Util.Hash.Hash.JenkinsHash;
			}
			else
			{
				if (ver == Version)
				{
					this.nbHash = @in.ReadInt();
					this.hashType = @in.ReadByte();
				}
				else
				{
					throw new IOException("Unsupported version: " + ver);
				}
			}
			this.vectorSize = @in.ReadInt();
			this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
		}
		//end class
	}
}
