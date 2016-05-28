using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	public class TestIntegerSplitter : TestCase
	{
		private long[] ToLongArray(IList<long> @in)
		{
			long[] @out = new long[@in.Count];
			for (int i = 0; i < @in.Count; i++)
			{
				@out[i] = @in[i];
			}
			return @out;
		}

		public virtual string FormatLongArray(long[] ar)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("[");
			bool first = true;
			foreach (long val in ar)
			{
				if (!first)
				{
					sb.Append(", ");
				}
				sb.Append(System.Convert.ToString(val));
				first = false;
			}
			sb.Append("]");
			return sb.ToString();
		}

		public virtual void AssertLongArrayEquals(long[] expected, long[] actual)
		{
			for (int i = 0; i < expected.Length; i++)
			{
				try
				{
					NUnit.Framework.Assert.AreEqual("Failure at position " + i + "; got " + actual[i]
						 + " instead of " + expected[i] + "; actual array is " + FormatLongArray(actual)
						, expected[i], actual[i]);
				}
				catch (IndexOutOfRangeException)
				{
					Fail("Expected array with " + expected.Length + " elements; got " + actual.Length
						 + ". Actual array is " + FormatLongArray(actual));
				}
			}
			if (actual.Length > expected.Length)
			{
				Fail("Actual array has " + actual.Length + " elements; expected " + expected.Length
					 + ". ACtual array is " + FormatLongArray(actual));
			}
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual void TestEvenSplits()
		{
			IList<long> splits = new IntegerSplitter().Split(10, 0, 100);
			long[] expected = new long[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
			AssertLongArrayEquals(expected, ToLongArray(splits));
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual void TestOddSplits()
		{
			IList<long> splits = new IntegerSplitter().Split(10, 0, 95);
			long[] expected = new long[] { 0, 9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 95 };
			AssertLongArrayEquals(expected, ToLongArray(splits));
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual void TestSingletonSplit()
		{
			IList<long> splits = new IntegerSplitter().Split(1, 5, 5);
			long[] expected = new long[] { 5, 5 };
			AssertLongArrayEquals(expected, ToLongArray(splits));
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual void TestSingletonSplit2()
		{
			// Same test, but overly-high numSplits
			IList<long> splits = new IntegerSplitter().Split(5, 5, 5);
			long[] expected = new long[] { 5, 5 };
			AssertLongArrayEquals(expected, ToLongArray(splits));
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual void TestTooManySplits()
		{
			IList<long> splits = new IntegerSplitter().Split(5, 3, 5);
			long[] expected = new long[] { 3, 4, 5 };
			AssertLongArrayEquals(expected, ToLongArray(splits));
		}
	}
}
