using Sharpen;

namespace org.apache.hadoop.util.bloom
{
	/// <summary>Implements a <i>dynamic Bloom filter</i>, as defined in the INFOCOM 2006 paper.
	/// 	</summary>
	/// <remarks>
	/// Implements a <i>dynamic Bloom filter</i>, as defined in the INFOCOM 2006 paper.
	/// <p>
	/// A dynamic Bloom filter (DBF) makes use of a <code>s * m</code> bit matrix but
	/// each of the <code>s</code> rows is a standard Bloom filter. The creation
	/// process of a DBF is iterative. At the start, the DBF is a <code>1 * m</code>
	/// bit matrix, i.e., it is composed of a single standard Bloom filter.
	/// It assumes that <code>n<sub>r</sub></code> elements are recorded in the
	/// initial bit vector, where <code>n<sub>r</sub> <= n&lt;/code> (<code>n</code> is
	/// the cardinality of the set <code>A</code> to record in the filter).
	/// <p>
	/// As the size of <code>A</code> grows during the execution of the application,
	/// several keys must be inserted in the DBF.  When inserting a key into the DBF,
	/// one must first get an active Bloom filter in the matrix.  A Bloom filter is
	/// active when the number of recorded keys, <code>n<sub>r</sub></code>, is
	/// strictly less than the current cardinality of <code>A</code>, <code>n</code>.
	/// If an active Bloom filter is found, the key is inserted and
	/// <code>n<sub>r</sub></code> is incremented by one. On the other hand, if there
	/// is no active Bloom filter, a new one is created (i.e., a new row is added to
	/// the matrix) according to the current size of <code>A</code> and the element
	/// is added in this new Bloom filter and the <code>n<sub>r</sub></code> value of
	/// this new Bloom filter is set to one.  A given key is said to belong to the
	/// DBF if the <code>k</code> positions are set to one in one of the matrix rows.
	/// <p>
	/// Originally created by
	/// <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
	/// </remarks>
	/// <seealso cref="Filter">The general behavior of a filter</seealso>
	/// <seealso cref="BloomFilter">A Bloom filter</seealso>
	/// <seealso><a href="http://www.cse.fau.edu/~jie/research/publications/Publication_files/infocom2006.pdf">Theory and Network Applications of Dynamic Bloom Filters</a>
	/// 	</seealso>
	public class DynamicBloomFilter : org.apache.hadoop.util.bloom.Filter
	{
		/// <summary>Threshold for the maximum number of key to record in a dynamic Bloom filter row.
		/// 	</summary>
		private int nr;

		/// <summary>The number of keys recorded in the current standard active Bloom filter.
		/// 	</summary>
		private int currentNbRecord;

		/// <summary>The matrix of Bloom filter.</summary>
		private org.apache.hadoop.util.bloom.BloomFilter[] matrix;

		/// <summary>Zero-args constructor for the serialization.</summary>
		public DynamicBloomFilter()
		{
		}

		/// <summary>Constructor.</summary>
		/// <remarks>
		/// Constructor.
		/// <p>
		/// Builds an empty Dynamic Bloom filter.
		/// </remarks>
		/// <param name="vectorSize">The number of bits in the vector.</param>
		/// <param name="nbHash">The number of hash function to consider.</param>
		/// <param name="hashType">
		/// type of the hashing function (see
		/// <see cref="org.apache.hadoop.util.hash.Hash"/>
		/// ).
		/// </param>
		/// <param name="nr">
		/// The threshold for the maximum number of keys to record in a
		/// dynamic Bloom filter row.
		/// </param>
		public DynamicBloomFilter(int vectorSize, int nbHash, int hashType, int nr)
			: base(vectorSize, nbHash, hashType)
		{
			this.nr = nr;
			this.currentNbRecord = 0;
			matrix = new org.apache.hadoop.util.bloom.BloomFilter[1];
			matrix[0] = new org.apache.hadoop.util.bloom.BloomFilter(this.vectorSize, this.nbHash
				, this.hashType);
		}

		public override void add(org.apache.hadoop.util.bloom.Key key)
		{
			if (key == null)
			{
				throw new System.ArgumentNullException("Key can not be null");
			}
			org.apache.hadoop.util.bloom.BloomFilter bf = getActiveStandardBF();
			if (bf == null)
			{
				addRow();
				bf = matrix[matrix.Length - 1];
				currentNbRecord = 0;
			}
			bf.add(key);
			currentNbRecord++;
		}

		public override void and(org.apache.hadoop.util.bloom.Filter filter)
		{
			if (filter == null || !(filter is org.apache.hadoop.util.bloom.DynamicBloomFilter
				) || filter.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new System.ArgumentException("filters cannot be and-ed");
			}
			org.apache.hadoop.util.bloom.DynamicBloomFilter dbf = (org.apache.hadoop.util.bloom.DynamicBloomFilter
				)filter;
			if (dbf.matrix.Length != this.matrix.Length || dbf.nr != this.nr)
			{
				throw new System.ArgumentException("filters cannot be and-ed");
			}
			for (int i = 0; i < matrix.Length; i++)
			{
				matrix[i].and(dbf.matrix[i]);
			}
		}

		public override bool membershipTest(org.apache.hadoop.util.bloom.Key key)
		{
			if (key == null)
			{
				return true;
			}
			for (int i = 0; i < matrix.Length; i++)
			{
				if (matrix[i].membershipTest(key))
				{
					return true;
				}
			}
			return false;
		}

		public override void not()
		{
			for (int i = 0; i < matrix.Length; i++)
			{
				matrix[i].not();
			}
		}

		public override void or(org.apache.hadoop.util.bloom.Filter filter)
		{
			if (filter == null || !(filter is org.apache.hadoop.util.bloom.DynamicBloomFilter
				) || filter.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new System.ArgumentException("filters cannot be or-ed");
			}
			org.apache.hadoop.util.bloom.DynamicBloomFilter dbf = (org.apache.hadoop.util.bloom.DynamicBloomFilter
				)filter;
			if (dbf.matrix.Length != this.matrix.Length || dbf.nr != this.nr)
			{
				throw new System.ArgumentException("filters cannot be or-ed");
			}
			for (int i = 0; i < matrix.Length; i++)
			{
				matrix[i].or(dbf.matrix[i]);
			}
		}

		public override void xor(org.apache.hadoop.util.bloom.Filter filter)
		{
			if (filter == null || !(filter is org.apache.hadoop.util.bloom.DynamicBloomFilter
				) || filter.vectorSize != this.vectorSize || filter.nbHash != this.nbHash)
			{
				throw new System.ArgumentException("filters cannot be xor-ed");
			}
			org.apache.hadoop.util.bloom.DynamicBloomFilter dbf = (org.apache.hadoop.util.bloom.DynamicBloomFilter
				)filter;
			if (dbf.matrix.Length != this.matrix.Length || dbf.nr != this.nr)
			{
				throw new System.ArgumentException("filters cannot be xor-ed");
			}
			for (int i = 0; i < matrix.Length; i++)
			{
				matrix[i].xor(dbf.matrix[i]);
			}
		}

		public override string ToString()
		{
			java.lang.StringBuilder res = new java.lang.StringBuilder();
			for (int i = 0; i < matrix.Length; i++)
			{
				res.Append(matrix[i]);
				res.Append(char.LINE_SEPARATOR);
			}
			return res.ToString();
		}

		// Writable
		/// <exception cref="System.IO.IOException"/>
		public override void write(java.io.DataOutput @out)
		{
			base.write(@out);
			@out.writeInt(nr);
			@out.writeInt(currentNbRecord);
			@out.writeInt(matrix.Length);
			for (int i = 0; i < matrix.Length; i++)
			{
				matrix[i].write(@out);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void readFields(java.io.DataInput @in)
		{
			base.readFields(@in);
			nr = @in.readInt();
			currentNbRecord = @in.readInt();
			int len = @in.readInt();
			matrix = new org.apache.hadoop.util.bloom.BloomFilter[len];
			for (int i = 0; i < matrix.Length; i++)
			{
				matrix[i] = new org.apache.hadoop.util.bloom.BloomFilter();
				matrix[i].readFields(@in);
			}
		}

		/// <summary>Adds a new row to <i>this</i> dynamic Bloom filter.</summary>
		private void addRow()
		{
			org.apache.hadoop.util.bloom.BloomFilter[] tmp = new org.apache.hadoop.util.bloom.BloomFilter
				[matrix.Length + 1];
			for (int i = 0; i < matrix.Length; i++)
			{
				tmp[i] = matrix[i];
			}
			tmp[tmp.Length - 1] = new org.apache.hadoop.util.bloom.BloomFilter(vectorSize, nbHash
				, hashType);
			matrix = tmp;
		}

		/// <summary>Returns the active standard Bloom filter in <i>this</i> dynamic Bloom filter.
		/// 	</summary>
		/// <returns>
		/// BloomFilter The active standard Bloom filter.
		/// <code>Null</code> otherwise.
		/// </returns>
		private org.apache.hadoop.util.bloom.BloomFilter getActiveStandardBF()
		{
			if (currentNbRecord >= nr)
			{
				return null;
			}
			return matrix[matrix.Length - 1];
		}
	}
}
