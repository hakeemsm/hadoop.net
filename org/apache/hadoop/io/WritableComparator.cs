using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// A Comparator for
	/// <see cref="WritableComparable{T}"/>
	/// s.
	/// <p>This base implemenation uses the natural ordering.  To define alternate
	/// orderings, override
	/// <see cref="compare(WritableComparable{T}, WritableComparable{T})"/>
	/// .
	/// <p>One may optimize compare-intensive operations by overriding
	/// <see cref="compare(byte[], int, int, byte[], int, int)"/>
	/// .  Static utility methods are
	/// provided to assist in optimized implementations of this method.
	/// </summary>
	public class WritableComparator : org.apache.hadoop.io.RawComparator, org.apache.hadoop.conf.Configurable
	{
		private static readonly java.util.concurrent.ConcurrentHashMap<java.lang.Class, org.apache.hadoop.io.WritableComparator
			> comparators = new java.util.concurrent.ConcurrentHashMap<java.lang.Class, org.apache.hadoop.io.WritableComparator
			>();

		private org.apache.hadoop.conf.Configuration conf;

		// registry
		/// <summary>For backwards compatibility.</summary>
		public static org.apache.hadoop.io.WritableComparator get(java.lang.Class c)
		{
			return get(c, null);
		}

		/// <summary>
		/// Get a comparator for a
		/// <see cref="WritableComparable{T}"/>
		/// implementation.
		/// </summary>
		public static org.apache.hadoop.io.WritableComparator get(java.lang.Class c, org.apache.hadoop.conf.Configuration
			 conf)
		{
			org.apache.hadoop.io.WritableComparator comparator = comparators[c];
			if (comparator == null)
			{
				// force the static initializers to run
				forceInit(c);
				// look to see if it is defined now
				comparator = comparators[c];
				// if not, use the generic one
				if (comparator == null)
				{
					comparator = new org.apache.hadoop.io.WritableComparator(c, conf, true);
				}
			}
			// Newly passed Configuration objects should be used.
			org.apache.hadoop.util.ReflectionUtils.setConf(comparator, conf);
			return comparator;
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		/// <summary>Force initialization of the static members.</summary>
		/// <remarks>
		/// Force initialization of the static members.
		/// As of Java 5, referencing a class doesn't force it to initialize. Since
		/// this class requires that the classes be initialized to declare their
		/// comparators, we force that initialization to happen.
		/// </remarks>
		/// <param name="cls">the class to initialize</param>
		private static void forceInit(java.lang.Class cls)
		{
			try
			{
				java.lang.Class.forName(cls.getName(), true, cls.getClassLoader());
			}
			catch (java.lang.ClassNotFoundException e)
			{
				throw new System.ArgumentException("Can't initialize class " + cls, e);
			}
		}

		/// <summary>
		/// Register an optimized comparator for a
		/// <see cref="WritableComparable{T}"/>
		/// implementation. Comparators registered with this method must be
		/// thread-safe.
		/// </summary>
		public static void define(java.lang.Class c, org.apache.hadoop.io.WritableComparator
			 comparator)
		{
			comparators[c] = comparator;
		}

		private readonly java.lang.Class keyClass;

		private readonly org.apache.hadoop.io.WritableComparable key1;

		private readonly org.apache.hadoop.io.WritableComparable key2;

		private readonly org.apache.hadoop.io.DataInputBuffer buffer;

		protected internal WritableComparator()
			: this(null)
		{
		}

		/// <summary>
		/// Construct for a
		/// <see cref="WritableComparable{T}"/>
		/// implementation.
		/// </summary>
		protected internal WritableComparator(java.lang.Class keyClass)
			: this(keyClass, null, false)
		{
		}

		protected internal WritableComparator(java.lang.Class keyClass, bool createInstances
			)
			: this(keyClass, null, createInstances)
		{
		}

		protected internal WritableComparator(java.lang.Class keyClass, org.apache.hadoop.conf.Configuration
			 conf, bool createInstances)
		{
			this.keyClass = keyClass;
			this.conf = (conf != null) ? conf : new org.apache.hadoop.conf.Configuration();
			if (createInstances)
			{
				key1 = newKey();
				key2 = newKey();
				buffer = new org.apache.hadoop.io.DataInputBuffer();
			}
			else
			{
				key1 = key2 = null;
				buffer = null;
			}
		}

		/// <summary>Returns the WritableComparable implementation class.</summary>
		public virtual java.lang.Class getKeyClass()
		{
			return keyClass;
		}

		/// <summary>
		/// Construct a new
		/// <see cref="WritableComparable{T}"/>
		/// instance.
		/// </summary>
		public virtual org.apache.hadoop.io.WritableComparable newKey()
		{
			return org.apache.hadoop.util.ReflectionUtils.newInstance(keyClass, conf);
		}

		/// <summary>Optimization hook.</summary>
		/// <remarks>
		/// Optimization hook.  Override this to make SequenceFile.Sorter's scream.
		/// <p>The default implementation reads the data into two
		/// <see cref="WritableComparable{T}"/>
		/// s (using
		/// <see cref="Writable.readFields(java.io.DataInput)"/>
		/// , then calls
		/// <see cref="compare(WritableComparable{T}, WritableComparable{T})"/>
		/// .
		/// </remarks>
		public virtual int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			try
			{
				buffer.reset(b1, s1, l1);
				// parse key1
				key1.readFields(buffer);
				buffer.reset(b2, s2, l2);
				// parse key2
				key2.readFields(buffer);
				buffer.reset(null, 0, 0);
			}
			catch (System.IO.IOException e)
			{
				// clean up reference
				throw new System.Exception(e);
			}
			return compare(key1, key2);
		}

		// compare them
		/// <summary>Compare two WritableComparables.</summary>
		/// <remarks>
		/// Compare two WritableComparables.
		/// <p> The default implementation uses the natural ordering, calling
		/// <see cref="java.lang.Comparable{T}.compareTo(object)"/>
		/// .
		/// </remarks>
		public virtual int compare(org.apache.hadoop.io.WritableComparable a, org.apache.hadoop.io.WritableComparable
			 b)
		{
			return a.compareTo(b);
		}

		public virtual int compare(object a, object b)
		{
			return compare((org.apache.hadoop.io.WritableComparable)a, (org.apache.hadoop.io.WritableComparable
				)b);
		}

		/// <summary>Lexicographic order of binary data.</summary>
		public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int 
			l2)
		{
			return org.apache.hadoop.io.FastByteComparisons.compareTo(b1, s1, l1, b2, s2, l2);
		}

		/// <summary>Compute hash for binary data.</summary>
		public static int hashBytes(byte[] bytes, int offset, int length)
		{
			int hash = 1;
			for (int i = offset; i < offset + length; i++)
			{
				hash = (31 * hash) + (int)bytes[i];
			}
			return hash;
		}

		/// <summary>Compute hash for binary data.</summary>
		public static int hashBytes(byte[] bytes, int length)
		{
			return hashBytes(bytes, 0, length);
		}

		/// <summary>Parse an unsigned short from a byte array.</summary>
		public static int readUnsignedShort(byte[] bytes, int start)
		{
			return (((bytes[start] & unchecked((int)(0xff))) << 8) + ((bytes[start + 1] & unchecked(
				(int)(0xff)))));
		}

		/// <summary>Parse an integer from a byte array.</summary>
		public static int readInt(byte[] bytes, int start)
		{
			return (((bytes[start] & unchecked((int)(0xff))) << 24) + ((bytes[start + 1] & unchecked(
				(int)(0xff))) << 16) + ((bytes[start + 2] & unchecked((int)(0xff))) << 8) + ((bytes
				[start + 3] & unchecked((int)(0xff)))));
		}

		/// <summary>Parse a float from a byte array.</summary>
		public static float readFloat(byte[] bytes, int start)
		{
			return Sharpen.Runtime.intBitsToFloat(readInt(bytes, start));
		}

		/// <summary>Parse a long from a byte array.</summary>
		public static long readLong(byte[] bytes, int start)
		{
			return ((long)(readInt(bytes, start)) << 32) + (readInt(bytes, start + 4) & unchecked(
				(long)(0xFFFFFFFFL)));
		}

		/// <summary>Parse a double from a byte array.</summary>
		public static double readDouble(byte[] bytes, int start)
		{
			return double.longBitsToDouble(readLong(bytes, start));
		}

		/// <summary>Reads a zero-compressed encoded long from a byte array and returns it.</summary>
		/// <param name="bytes">byte array with decode long</param>
		/// <param name="start">starting index</param>
		/// <exception cref="System.IO.IOException"></exception>
		/// <returns>deserialized long</returns>
		public static long readVLong(byte[] bytes, int start)
		{
			int len = bytes[start];
			if (len >= -112)
			{
				return len;
			}
			bool isNegative = (len < -120);
			len = isNegative ? -(len + 120) : -(len + 112);
			if (start + 1 + len > bytes.Length)
			{
				throw new System.IO.IOException("Not enough number of bytes for a zero-compressed integer"
					);
			}
			long i = 0;
			for (int idx = 0; idx < len; idx++)
			{
				i = i << 8;
				i = i | (bytes[start + 1 + idx] & unchecked((int)(0xFF)));
			}
			return (isNegative ? (i ^ -1L) : i);
		}

		/// <summary>Reads a zero-compressed encoded integer from a byte array and returns it.
		/// 	</summary>
		/// <param name="bytes">byte array with the encoded integer</param>
		/// <param name="start">start index</param>
		/// <exception cref="System.IO.IOException"></exception>
		/// <returns>deserialized integer</returns>
		public static int readVInt(byte[] bytes, int start)
		{
			return (int)readVLong(bytes, start);
		}
	}
}
