using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Utility code to do optimized byte-array comparison.</summary>
	/// <remarks>
	/// Utility code to do optimized byte-array comparison.
	/// This is borrowed and slightly modified from Guava's
	/// <see cref="com.google.common.primitives.UnsignedBytes"/>
	/// class to be able to compare arrays that start at non-zero offsets.
	/// </remarks>
	internal abstract class FastByteComparisons
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.FastByteComparisons
			)));

		/// <summary>Lexicographically compare two byte arrays.</summary>
		public static int compareTo(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			return org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.BEST_COMPARER
				.compareTo(b1, s1, l1, b2, s2, l2);
		}

		private interface Comparer<T>
		{
			int compareTo(T buffer1, int offset1, int length1, T buffer2, int offset2, int length2
				);
		}

		private static org.apache.hadoop.io.FastByteComparisons.Comparer<byte[]> lexicographicalComparerJavaImpl
			()
		{
			return org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.PureJavaComparer
				.INSTANCE;
		}

		/// <summary>
		/// Provides a lexicographical comparer implementation; either a Java
		/// implementation or a faster implementation based on
		/// <see cref="sun.misc.Unsafe"/>
		/// .
		/// <p>Uses reflection to gracefully fall back to the Java implementation if
		/// <c>Unsafe</c>
		/// isn't available.
		/// </summary>
		private class LexicographicalComparerHolder
		{
			internal static readonly string UNSAFE_COMPARER_NAME = Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder))
				.getName() + "$UnsafeComparer";

			internal static readonly org.apache.hadoop.io.FastByteComparisons.Comparer<byte[]
				> BEST_COMPARER = getBestComparer();

			/// <summary>
			/// Returns the Unsafe-using Comparer, or falls back to the pure-Java
			/// implementation if unable to do so.
			/// </summary>
			internal static org.apache.hadoop.io.FastByteComparisons.Comparer<byte[]> getBestComparer
				()
			{
				if (Sharpen.Runtime.getProperty("os.arch").Equals("sparc"))
				{
					if (LOG.isTraceEnabled())
					{
						LOG.trace("Lexicographical comparer selected for " + "byte aligned system architecture"
							);
					}
					return lexicographicalComparerJavaImpl();
				}
				try
				{
					java.lang.Class theClass = java.lang.Class.forName(UNSAFE_COMPARER_NAME);
					// yes, UnsafeComparer does implement Comparer<byte[]>
					org.apache.hadoop.io.FastByteComparisons.Comparer<byte[]> comparer = (org.apache.hadoop.io.FastByteComparisons.Comparer
						<byte[]>)theClass.getEnumConstants()[0];
					if (LOG.isTraceEnabled())
					{
						LOG.trace("Unsafe comparer selected for " + "byte unaligned system architecture");
					}
					return comparer;
				}
				catch (System.Exception t)
				{
					// ensure we really catch *everything*
					if (LOG.isTraceEnabled())
					{
						LOG.trace(t.Message);
						LOG.trace("Lexicographical comparer selected");
					}
					return lexicographicalComparerJavaImpl();
				}
			}

			[System.Serializable]
			private sealed class PureJavaComparer : org.apache.hadoop.io.FastByteComparisons.Comparer
				<byte[]>
			{
				public static readonly org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.PureJavaComparer
					 INSTANCE = new org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.PureJavaComparer
					();

				public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int
					 offset2, int length2)
				{
					// Short circuit equal case
					if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2)
					{
						return 0;
					}
					// Bring WritableComparator code local
					int end1 = offset1 + length1;
					int end2 = offset2 + length2;
					for (int i = offset1; i < end1 && j < end2; i++, j++)
					{
						int a = (buffer1[i] & unchecked((int)(0xff)));
						int b = (buffer2[j] & unchecked((int)(0xff)));
						if (a != b)
						{
							return a - b;
						}
					}
					return length1 - length2;
				}
			}

			[System.Serializable]
			private sealed class UnsafeComparer : org.apache.hadoop.io.FastByteComparisons.Comparer
				<byte[]>
			{
				public static readonly org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
					 INSTANCE = new org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
					();

				internal static readonly sun.misc.Unsafe theUnsafe;

				/// <summary>The offset to the first element in a byte array.</summary>
				internal static readonly int BYTE_ARRAY_BASE_OFFSET;

				static UnsafeComparer()
				{
					// used via reflection
					org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
						.theUnsafe = (sun.misc.Unsafe)java.security.AccessController.doPrivileged(new _PrivilegedAction_143
						());
					// It doesn't matter what we throw;
					// it's swallowed in getBestComparer().
					org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
						.BYTE_ARRAY_BASE_OFFSET = org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
						.theUnsafe.arrayBaseOffset(Sharpen.Runtime.getClassForType(typeof(byte[])));
					// sanity check - this should never fail
					if (org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
						.theUnsafe.arrayIndexScale(Sharpen.Runtime.getClassForType(typeof(byte[]))) != 1)
					{
						throw new java.lang.AssertionError();
					}
				}

				private sealed class _PrivilegedAction_143 : java.security.PrivilegedAction<object
					>
				{
					public _PrivilegedAction_143()
					{
					}

					public object run()
					{
						try
						{
							java.lang.reflect.Field f = Sharpen.Runtime.getClassForType(typeof(sun.misc.Unsafe
								)).getDeclaredField("theUnsafe");
							f.setAccessible(true);
							return f.get(null);
						}
						catch (java.lang.NoSuchFieldException)
						{
							throw new System.Exception();
						}
						catch (java.lang.IllegalAccessException)
						{
							throw new System.Exception();
						}
					}
				}

				internal static readonly bool littleEndian = java.nio.ByteOrder.nativeOrder().Equals
					(java.nio.ByteOrder.LITTLE_ENDIAN);

				/// <summary>
				/// Returns true if x1 is less than x2, when both values are treated as
				/// unsigned.
				/// </summary>
				internal static bool lessThanUnsigned(long x1, long x2)
				{
					return (x1 + long.MinValue) < (x2 + long.MinValue);
				}

				/// <summary>Lexicographically compare two arrays.</summary>
				/// <param name="buffer1">left operand</param>
				/// <param name="buffer2">right operand</param>
				/// <param name="offset1">Where to start comparing in the left buffer</param>
				/// <param name="offset2">Where to start comparing in the right buffer</param>
				/// <param name="length1">How much to compare from the left buffer</param>
				/// <param name="length2">How much to compare from the right buffer</param>
				/// <returns>0 if equal, &lt; 0 if left is less than right, etc.</returns>
				public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int
					 offset2, int length2)
				{
					// Short circuit equal case
					if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2)
					{
						return 0;
					}
					int minLength = System.Math.min(length1, length2);
					int minWords = minLength / com.google.common.primitives.Longs.BYTES;
					int offset1Adj = offset1 + org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
						.BYTE_ARRAY_BASE_OFFSET;
					int offset2Adj = offset2 + org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
						.BYTE_ARRAY_BASE_OFFSET;
					/*
					* Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
					* time is no slower than comparing 4 bytes at a time even on 32-bit.
					* On the other hand, it is substantially faster on 64-bit.
					*/
					for (int i = 0; i < minWords * com.google.common.primitives.Longs.BYTES; i += com.google.common.primitives.Longs
						.BYTES)
					{
						long lw = org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
							.theUnsafe.getLong(buffer1, offset1Adj + (long)i);
						long rw = org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
							.theUnsafe.getLong(buffer2, offset2Adj + (long)i);
						long diff = lw ^ rw;
						if (diff != 0)
						{
							if (!org.apache.hadoop.io.FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
								.littleEndian)
							{
								return lessThanUnsigned(lw, rw) ? -1 : 1;
							}
							// Use binary search
							int n = 0;
							int y;
							int x = (int)diff;
							if (x == 0)
							{
								x = (int)((long)(((ulong)diff) >> 32));
								n = 32;
							}
							y = x << 16;
							if (y == 0)
							{
								n += 16;
							}
							else
							{
								x = y;
							}
							y = x << 8;
							if (y == 0)
							{
								n += 8;
							}
							return (int)((((long)(((ulong)lw) >> n)) & unchecked((long)(0xFFL))) - (((long)((
								(ulong)rw) >> n)) & unchecked((long)(0xFFL))));
						}
					}
					// The epilogue to cover the last (minLength % 8) elements.
					for (int i_1 = minWords * com.google.common.primitives.Longs.BYTES; i_1 < minLength
						; i_1++)
					{
						int result = com.google.common.primitives.UnsignedBytes.compare(buffer1[offset1 +
							 i_1], buffer2[offset2 + i_1]);
						if (result != 0)
						{
							return result;
						}
					}
					return length1 - length2;
				}
			}
		}
	}
}
