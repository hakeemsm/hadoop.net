using System;
using System.Reflection;
using Com.Google.Common.Primitives;
using Org.Apache.Commons.Logging;

using Sun.Misc;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Utility code to do optimized byte-array comparison.</summary>
	/// <remarks>
	/// Utility code to do optimized byte-array comparison.
	/// This is borrowed and slightly modified from Guava's
	/// <see cref="Com.Google.Common.Primitives.UnsignedBytes"/>
	/// class to be able to compare arrays that start at non-zero offsets.
	/// </remarks>
	internal abstract class FastByteComparisons
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(FastByteComparisons));

		/// <summary>Lexicographically compare two byte arrays.</summary>
		public static int CompareTo(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			return FastByteComparisons.LexicographicalComparerHolder.BestComparer.CompareTo(b1
				, s1, l1, b2, s2, l2);
		}

		private interface Comparer<T>
		{
			int CompareTo(T buffer1, int offset1, int length1, T buffer2, int offset2, int length2
				);
		}

		private static FastByteComparisons.Comparer<byte[]> LexicographicalComparerJavaImpl
			()
		{
			return FastByteComparisons.LexicographicalComparerHolder.PureJavaComparer.Instance;
		}

		/// <summary>
		/// Provides a lexicographical comparer implementation; either a Java
		/// implementation or a faster implementation based on
		/// <see cref="Sun.Misc.Unsafe"/>
		/// .
		/// <p>Uses reflection to gracefully fall back to the Java implementation if
		/// <c>Unsafe</c>
		/// isn't available.
		/// </summary>
		private class LexicographicalComparerHolder
		{
			internal static readonly string UnsafeComparerName = typeof(FastByteComparisons.LexicographicalComparerHolder
				).FullName + "$UnsafeComparer";

			internal static readonly FastByteComparisons.Comparer<byte[]> BestComparer = GetBestComparer
				();

			/// <summary>
			/// Returns the Unsafe-using Comparer, or falls back to the pure-Java
			/// implementation if unable to do so.
			/// </summary>
			internal static FastByteComparisons.Comparer<byte[]> GetBestComparer()
			{
				if (Runtime.GetProperty("os.arch").Equals("sparc"))
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("Lexicographical comparer selected for " + "byte aligned system architecture"
							);
					}
					return LexicographicalComparerJavaImpl();
				}
				try
				{
					Type theClass = Runtime.GetType(UnsafeComparerName);
					// yes, UnsafeComparer does implement Comparer<byte[]>
					FastByteComparisons.Comparer<byte[]> comparer = (FastByteComparisons.Comparer<byte
						[]>)theClass.GetEnumConstants()[0];
					if (Log.IsTraceEnabled())
					{
						Log.Trace("Unsafe comparer selected for " + "byte unaligned system architecture");
					}
					return comparer;
				}
				catch (Exception t)
				{
					// ensure we really catch *everything*
					if (Log.IsTraceEnabled())
					{
						Log.Trace(t.Message);
						Log.Trace("Lexicographical comparer selected");
					}
					return LexicographicalComparerJavaImpl();
				}
			}

			[System.Serializable]
			private sealed class PureJavaComparer : FastByteComparisons.Comparer<byte[]>
			{
				public static readonly FastByteComparisons.LexicographicalComparerHolder.PureJavaComparer
					 Instance = new FastByteComparisons.LexicographicalComparerHolder.PureJavaComparer
					();

				public int CompareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int
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
			private sealed class UnsafeComparer : FastByteComparisons.Comparer<byte[]>
			{
				public static readonly FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
					 Instance = new FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
					();

				internal static readonly Unsafe theUnsafe;

				/// <summary>The offset to the first element in a byte array.</summary>
				internal static readonly int ByteArrayBaseOffset;

				static UnsafeComparer()
				{
					// used via reflection
					FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer.theUnsafe = (Unsafe
						)AccessController.DoPrivileged(new _PrivilegedAction_143());
					// It doesn't matter what we throw;
					// it's swallowed in getBestComparer().
					FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer.ByteArrayBaseOffset
						 = FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer.theUnsafe.ArrayBaseOffset
						(typeof(byte[]));
					// sanity check - this should never fail
					if (FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer.theUnsafe.ArrayIndexScale
						(typeof(byte[])) != 1)
					{
						throw new Exception();
					}
				}

				private sealed class _PrivilegedAction_143 : PrivilegedAction<object>
				{
					public _PrivilegedAction_143()
					{
					}

					public object Run()
					{
						try
						{
							FieldInfo f = Runtime.GetDeclaredField(typeof(Unsafe), "theUnsafe");
							return f.GetValue(null);
						}
						catch (NoSuchFieldException)
						{
							throw new Error();
						}
						catch (MemberAccessException)
						{
							throw new Error();
						}
					}
				}

				internal static readonly bool littleEndian = ByteOrder.NativeOrder().Equals(ByteOrder
					.LittleEndian);

				/// <summary>
				/// Returns true if x1 is less than x2, when both values are treated as
				/// unsigned.
				/// </summary>
				internal static bool LessThanUnsigned(long x1, long x2)
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
				public int CompareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int
					 offset2, int length2)
				{
					// Short circuit equal case
					if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2)
					{
						return 0;
					}
					int minLength = Math.Min(length1, length2);
					int minWords = minLength / Longs.Bytes;
					int offset1Adj = offset1 + FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
						.ByteArrayBaseOffset;
					int offset2Adj = offset2 + FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer
						.ByteArrayBaseOffset;
					/*
					* Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
					* time is no slower than comparing 4 bytes at a time even on 32-bit.
					* On the other hand, it is substantially faster on 64-bit.
					*/
					for (int i = 0; i < minWords * Longs.Bytes; i += Longs.Bytes)
					{
						long lw = FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer.theUnsafe
							.GetLong(buffer1, offset1Adj + (long)i);
						long rw = FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer.theUnsafe
							.GetLong(buffer2, offset2Adj + (long)i);
						long diff = lw ^ rw;
						if (diff != 0)
						{
							if (!FastByteComparisons.LexicographicalComparerHolder.UnsafeComparer.littleEndian)
							{
								return LessThanUnsigned(lw, rw) ? -1 : 1;
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
					for (int i_1 = minWords * Longs.Bytes; i_1 < minLength; i_1++)
					{
						int result = UnsignedBytes.Compare(buffer1[offset1 + i_1], buffer2[offset2 + i_1]
							);
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
