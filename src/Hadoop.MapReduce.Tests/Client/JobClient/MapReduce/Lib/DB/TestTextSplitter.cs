using System;
using System.Collections.Generic;
using System.Text;
using Java.Math;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	public class TestTextSplitter : TestCase
	{
		public virtual string FormatArray(object[] ar)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("[");
			bool first = true;
			foreach (object val in ar)
			{
				if (!first)
				{
					sb.Append(", ");
				}
				sb.Append(val.ToString());
				first = false;
			}
			sb.Append("]");
			return sb.ToString();
		}

		public virtual void AssertArrayEquals(object[] expected, object[] actual)
		{
			for (int i = 0; i < expected.Length; i++)
			{
				try
				{
					NUnit.Framework.Assert.AreEqual("Failure at position " + i + "; got " + actual[i]
						 + " instead of " + expected[i] + "; actual array is " + FormatArray(actual), expected
						[i], actual[i]);
				}
				catch (IndexOutOfRangeException)
				{
					Fail("Expected array with " + expected.Length + " elements; got " + actual.Length
						 + ". Actual array is " + FormatArray(actual));
				}
			}
			if (actual.Length > expected.Length)
			{
				Fail("Actual array has " + actual.Length + " elements; expected " + expected.Length
					 + ". Actual array is " + FormatArray(actual));
			}
		}

		public virtual void TestStringConvertEmpty()
		{
			TextSplitter splitter = new TextSplitter();
			BigDecimal emptyBigDec = splitter.StringToBigDecimal(string.Empty);
			NUnit.Framework.Assert.AreEqual(BigDecimal.Zero, emptyBigDec);
		}

		public virtual void TestBigDecConvertEmpty()
		{
			TextSplitter splitter = new TextSplitter();
			string emptyStr = splitter.BigDecimalToString(BigDecimal.Zero);
			NUnit.Framework.Assert.AreEqual(string.Empty, emptyStr);
		}

		public virtual void TestConvertA()
		{
			TextSplitter splitter = new TextSplitter();
			string @out = splitter.BigDecimalToString(splitter.StringToBigDecimal("A"));
			NUnit.Framework.Assert.AreEqual("A", @out);
		}

		public virtual void TestConvertZ()
		{
			TextSplitter splitter = new TextSplitter();
			string @out = splitter.BigDecimalToString(splitter.StringToBigDecimal("Z"));
			NUnit.Framework.Assert.AreEqual("Z", @out);
		}

		public virtual void TestConvertThreeChars()
		{
			TextSplitter splitter = new TextSplitter();
			string @out = splitter.BigDecimalToString(splitter.StringToBigDecimal("abc"));
			NUnit.Framework.Assert.AreEqual("abc", @out);
		}

		public virtual void TestConvertStr()
		{
			TextSplitter splitter = new TextSplitter();
			string @out = splitter.BigDecimalToString(splitter.StringToBigDecimal("big str"));
			NUnit.Framework.Assert.AreEqual("big str", @out);
		}

		public virtual void TestConvertChomped()
		{
			TextSplitter splitter = new TextSplitter();
			string @out = splitter.BigDecimalToString(splitter.StringToBigDecimal("AVeryLongStringIndeed"
				));
			NUnit.Framework.Assert.AreEqual("AVeryLon", @out);
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual void TestAlphabetSplit()
		{
			// This should give us 25 splits, one per letter.
			TextSplitter splitter = new TextSplitter();
			IList<string> splits = splitter.Split(25, "A", "Z", string.Empty);
			string[] expected = new string[] { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J"
				, "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
				 };
			AssertArrayEquals(expected, Sharpen.Collections.ToArray(splits, new string[0]));
		}

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual void TestCommonPrefix()
		{
			// Splits between 'Hand' and 'Hardy'
			TextSplitter splitter = new TextSplitter();
			IList<string> splits = splitter.Split(5, "nd", "rdy", "Ha");
			// Don't check for exact values in the middle, because the splitter generates some
			// ugly Unicode-isms. But do check that we get multiple splits and that it starts
			// and ends on the correct points.
			NUnit.Framework.Assert.AreEqual("Hand", splits[0]);
			NUnit.Framework.Assert.AreEqual("Hardy", splits[splits.Count - 1]);
			NUnit.Framework.Assert.AreEqual(6, splits.Count);
		}
	}
}
