using System;
using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// A Comparator for
	/// <see cref="WritableComparable{T}"/>
	/// s.
	/// <p>This base implemenation uses the natural ordering.  To define alternate
	/// orderings, override
	/// <see cref="Compare(WritableComparable{T}, WritableComparable{T})"/>
	/// .
	/// <p>One may optimize compare-intensive operations by overriding
	/// <see cref="Compare(byte[], int, int, byte[], int, int)"/>
	/// .  Static utility methods are
	/// provided to assist in optimized implementations of this method.
	/// </summary>
	public class WritableComparator : RawComparator, Configurable
	{
		private static readonly ConcurrentHashMap<Type, Org.Apache.Hadoop.IO.WritableComparator
			> comparators = new ConcurrentHashMap<Type, Org.Apache.Hadoop.IO.WritableComparator
			>();

		private Configuration conf;

		// registry
		/// <summary>For backwards compatibility.</summary>
		public static Org.Apache.Hadoop.IO.WritableComparator Get(Type c)
		{
			return Get(c, null);
		}

		/// <summary>
		/// Get a comparator for a
		/// <see cref="WritableComparable{T}"/>
		/// implementation.
		/// </summary>
		public static Org.Apache.Hadoop.IO.WritableComparator Get(Type c, Configuration conf
			)
		{
			Org.Apache.Hadoop.IO.WritableComparator comparator = comparators[c];
			if (comparator == null)
			{
				// force the static initializers to run
				ForceInit(c);
				// look to see if it is defined now
				comparator = comparators[c];
				// if not, use the generic one
				if (comparator == null)
				{
					comparator = new Org.Apache.Hadoop.IO.WritableComparator(c, conf, true);
				}
			}
			// Newly passed Configuration objects should be used.
			ReflectionUtils.SetConf(comparator, conf);
			return comparator;
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
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
		private static void ForceInit(Type cls)
		{
			try
			{
				Sharpen.Runtime.GetType(cls.FullName, true, cls.GetClassLoader());
			}
			catch (TypeLoadException e)
			{
				throw new ArgumentException("Can't initialize class " + cls, e);
			}
		}

		/// <summary>
		/// Register an optimized comparator for a
		/// <see cref="WritableComparable{T}"/>
		/// implementation. Comparators registered with this method must be
		/// thread-safe.
		/// </summary>
		public static void Define(Type c, Org.Apache.Hadoop.IO.WritableComparator comparator
			)
		{
			comparators[c] = comparator;
		}

		private readonly Type keyClass;

		private readonly WritableComparable key1;

		private readonly WritableComparable key2;

		private readonly DataInputBuffer buffer;

		protected internal WritableComparator()
			: this(null)
		{
		}

		/// <summary>
		/// Construct for a
		/// <see cref="WritableComparable{T}"/>
		/// implementation.
		/// </summary>
		protected internal WritableComparator(Type keyClass)
			: this(keyClass, null, false)
		{
		}

		protected internal WritableComparator(Type keyClass, bool createInstances)
			: this(keyClass, null, createInstances)
		{
		}

		protected internal WritableComparator(Type keyClass, Configuration conf, bool createInstances
			)
		{
			this.keyClass = keyClass;
			this.conf = (conf != null) ? conf : new Configuration();
			if (createInstances)
			{
				key1 = NewKey();
				key2 = NewKey();
				buffer = new DataInputBuffer();
			}
			else
			{
				key1 = key2 = null;
				buffer = null;
			}
		}

		/// <summary>Returns the WritableComparable implementation class.</summary>
		public virtual Type GetKeyClass()
		{
			return keyClass;
		}

		/// <summary>
		/// Construct a new
		/// <see cref="WritableComparable{T}"/>
		/// instance.
		/// </summary>
		public virtual WritableComparable NewKey()
		{
			return ReflectionUtils.NewInstance(keyClass, conf);
		}

		/// <summary>Optimization hook.</summary>
		/// <remarks>
		/// Optimization hook.  Override this to make SequenceFile.Sorter's scream.
		/// <p>The default implementation reads the data into two
		/// <see cref="WritableComparable{T}"/>
		/// s (using
		/// <see cref="Writable.ReadFields(System.IO.DataInput)"/>
		/// , then calls
		/// <see cref="Compare(WritableComparable{T}, WritableComparable{T})"/>
		/// .
		/// </remarks>
		public virtual int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			try
			{
				buffer.Reset(b1, s1, l1);
				// parse key1
				key1.ReadFields(buffer);
				buffer.Reset(b2, s2, l2);
				// parse key2
				key2.ReadFields(buffer);
				buffer.Reset(null, 0, 0);
			}
			catch (IOException e)
			{
				// clean up reference
				throw new RuntimeException(e);
			}
			return Compare(key1, key2);
		}

		// compare them
		/// <summary>Compare two WritableComparables.</summary>
		/// <remarks>
		/// Compare two WritableComparables.
		/// <p> The default implementation uses the natural ordering, calling
		/// <see cref="System.IComparable{T}.CompareTo(object)"/>
		/// .
		/// </remarks>
		public virtual int Compare(WritableComparable a, WritableComparable b)
		{
			return a.CompareTo(b);
		}

		public virtual int Compare(object a, object b)
		{
			return Compare((WritableComparable)a, (WritableComparable)b);
		}

		/// <summary>Lexicographic order of binary data.</summary>
		public static int CompareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int 
			l2)
		{
			return FastByteComparisons.CompareTo(b1, s1, l1, b2, s2, l2);
		}

		/// <summary>Compute hash for binary data.</summary>
		public static int HashBytes(byte[] bytes, int offset, int length)
		{
			int hash = 1;
			for (int i = offset; i < offset + length; i++)
			{
				hash = (31 * hash) + (int)bytes[i];
			}
			return hash;
		}

		/// <summary>Compute hash for binary data.</summary>
		public static int HashBytes(byte[] bytes, int length)
		{
			return HashBytes(bytes, 0, length);
		}

		/// <summary>Parse an unsigned short from a byte array.</summary>
		public static int ReadUnsignedShort(byte[] bytes, int start)
		{
			return (((bytes[start] & unchecked((int)(0xff))) << 8) + ((bytes[start + 1] & unchecked(
				(int)(0xff)))));
		}

		/// <summary>Parse an integer from a byte array.</summary>
		public static int ReadInt(byte[] bytes, int start)
		{
			return (((bytes[start] & unchecked((int)(0xff))) << 24) + ((bytes[start + 1] & unchecked(
				(int)(0xff))) << 16) + ((bytes[start + 2] & unchecked((int)(0xff))) << 8) + ((bytes
				[start + 3] & unchecked((int)(0xff)))));
		}

		/// <summary>Parse a float from a byte array.</summary>
		public static float ReadFloat(byte[] bytes, int start)
		{
			return Sharpen.Runtime.IntBitsToFloat(ReadInt(bytes, start));
		}

		/// <summary>Parse a long from a byte array.</summary>
		public static long ReadLong(byte[] bytes, int start)
		{
			return ((long)(ReadInt(bytes, start)) << 32) + (ReadInt(bytes, start + 4) & unchecked(
				(long)(0xFFFFFFFFL)));
		}

		/// <summary>Parse a double from a byte array.</summary>
		public static double ReadDouble(byte[] bytes, int start)
		{
			return double.LongBitsToDouble(ReadLong(bytes, start));
		}

		/// <summary>Reads a zero-compressed encoded long from a byte array and returns it.</summary>
		/// <param name="bytes">byte array with decode long</param>
		/// <param name="start">starting index</param>
		/// <exception cref="System.IO.IOException"></exception>
		/// <returns>deserialized long</returns>
		public static long ReadVLong(byte[] bytes, int start)
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
				throw new IOException("Not enough number of bytes for a zero-compressed integer");
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
		public static int ReadVInt(byte[] bytes, int start)
		{
			return (int)ReadVLong(bytes, start);
		}
	}
}
