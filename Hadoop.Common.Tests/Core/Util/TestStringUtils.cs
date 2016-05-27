using System;
using System.Collections.Generic;
using System.Globalization;
using NUnit.Framework;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestStringUtils : UnitTestcaseTimeLimit
	{
		private static readonly string NullStr = null;

		private const string EmptyStr = string.Empty;

		private const string StrWoSpecialChars = "AB";

		private const string StrWithComma = "A,B";

		private const string EscapedStrWithComma = "A\\,B";

		private const string StrWithEscape = "AB\\";

		private const string EscapedStrWithEscape = "AB\\\\";

		private const string StrWithBoth2 = ",A\\,,B\\\\,";

		private const string EscapedStrWithBoth2 = "\\,A\\\\\\,\\,B\\\\\\\\\\,";

		/// <exception cref="System.Exception"/>
		public virtual void TestEscapeString()
		{
			NUnit.Framework.Assert.AreEqual(NullStr, StringUtils.EscapeString(NullStr));
			NUnit.Framework.Assert.AreEqual(EmptyStr, StringUtils.EscapeString(EmptyStr));
			NUnit.Framework.Assert.AreEqual(StrWoSpecialChars, StringUtils.EscapeString(StrWoSpecialChars
				));
			NUnit.Framework.Assert.AreEqual(EscapedStrWithComma, StringUtils.EscapeString(StrWithComma
				));
			NUnit.Framework.Assert.AreEqual(EscapedStrWithEscape, StringUtils.EscapeString(StrWithEscape
				));
			NUnit.Framework.Assert.AreEqual(EscapedStrWithBoth2, StringUtils.EscapeString(StrWithBoth2
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSplit()
		{
			NUnit.Framework.Assert.AreEqual(NullStr, StringUtils.Split(NullStr));
			string[] splits = StringUtils.Split(EmptyStr);
			NUnit.Framework.Assert.AreEqual(0, splits.Length);
			splits = StringUtils.Split(",,");
			NUnit.Framework.Assert.AreEqual(0, splits.Length);
			splits = StringUtils.Split(StrWoSpecialChars);
			NUnit.Framework.Assert.AreEqual(1, splits.Length);
			NUnit.Framework.Assert.AreEqual(StrWoSpecialChars, splits[0]);
			splits = StringUtils.Split(StrWithComma);
			NUnit.Framework.Assert.AreEqual(2, splits.Length);
			NUnit.Framework.Assert.AreEqual("A", splits[0]);
			NUnit.Framework.Assert.AreEqual("B", splits[1]);
			splits = StringUtils.Split(EscapedStrWithComma);
			NUnit.Framework.Assert.AreEqual(1, splits.Length);
			NUnit.Framework.Assert.AreEqual(EscapedStrWithComma, splits[0]);
			splits = StringUtils.Split(StrWithEscape);
			NUnit.Framework.Assert.AreEqual(1, splits.Length);
			NUnit.Framework.Assert.AreEqual(StrWithEscape, splits[0]);
			splits = StringUtils.Split(StrWithBoth2);
			NUnit.Framework.Assert.AreEqual(3, splits.Length);
			NUnit.Framework.Assert.AreEqual(EmptyStr, splits[0]);
			NUnit.Framework.Assert.AreEqual("A\\,", splits[1]);
			NUnit.Framework.Assert.AreEqual("B\\\\", splits[2]);
			splits = StringUtils.Split(EscapedStrWithBoth2);
			NUnit.Framework.Assert.AreEqual(1, splits.Length);
			NUnit.Framework.Assert.AreEqual(EscapedStrWithBoth2, splits[0]);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleSplit()
		{
			string[] ToTest = new string[] { "a/b/c", "a/b/c////", "///a/b/c", string.Empty, 
				"/", "////" };
			foreach (string testSubject in ToTest)
			{
				Assert.AssertArrayEquals("Testing '" + testSubject + "'", testSubject.Split("/"), 
					StringUtils.Split(testSubject, '/'));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestUnescapeString()
		{
			NUnit.Framework.Assert.AreEqual(NullStr, StringUtils.UnEscapeString(NullStr));
			NUnit.Framework.Assert.AreEqual(EmptyStr, StringUtils.UnEscapeString(EmptyStr));
			NUnit.Framework.Assert.AreEqual(StrWoSpecialChars, StringUtils.UnEscapeString(StrWoSpecialChars
				));
			try
			{
				StringUtils.UnEscapeString(StrWithComma);
				NUnit.Framework.Assert.Fail("Should throw IllegalArgumentException");
			}
			catch (ArgumentException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual(StrWithComma, StringUtils.UnEscapeString(EscapedStrWithComma
				));
			try
			{
				StringUtils.UnEscapeString(StrWithEscape);
				NUnit.Framework.Assert.Fail("Should throw IllegalArgumentException");
			}
			catch (ArgumentException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual(StrWithEscape, StringUtils.UnEscapeString(EscapedStrWithEscape
				));
			try
			{
				StringUtils.UnEscapeString(StrWithBoth2);
				NUnit.Framework.Assert.Fail("Should throw IllegalArgumentException");
			}
			catch (ArgumentException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual(StrWithBoth2, StringUtils.UnEscapeString(EscapedStrWithBoth2
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTraditionalBinaryPrefix()
		{
			//test string2long(..)
			string[] symbol = new string[] { "k", "m", "g", "t", "p", "e" };
			long m = 1024;
			foreach (string s in symbol)
			{
				NUnit.Framework.Assert.AreEqual(0, StringUtils.TraditionalBinaryPrefix.String2long
					(0 + s));
				NUnit.Framework.Assert.AreEqual(m, StringUtils.TraditionalBinaryPrefix.String2long
					(1 + s));
				m *= 1024;
			}
			NUnit.Framework.Assert.AreEqual(0L, StringUtils.TraditionalBinaryPrefix.String2long
				("0"));
			NUnit.Framework.Assert.AreEqual(1024L, StringUtils.TraditionalBinaryPrefix.String2long
				("1k"));
			NUnit.Framework.Assert.AreEqual(-1024L, StringUtils.TraditionalBinaryPrefix.String2long
				("-1k"));
			NUnit.Framework.Assert.AreEqual(1259520L, StringUtils.TraditionalBinaryPrefix.String2long
				("1230K"));
			NUnit.Framework.Assert.AreEqual(-1259520L, StringUtils.TraditionalBinaryPrefix.String2long
				("-1230K"));
			NUnit.Framework.Assert.AreEqual(104857600L, StringUtils.TraditionalBinaryPrefix.String2long
				("100m"));
			NUnit.Framework.Assert.AreEqual(-104857600L, StringUtils.TraditionalBinaryPrefix.String2long
				("-100M"));
			NUnit.Framework.Assert.AreEqual(956703965184L, StringUtils.TraditionalBinaryPrefix.String2long
				("891g"));
			NUnit.Framework.Assert.AreEqual(-956703965184L, StringUtils.TraditionalBinaryPrefix.String2long
				("-891G"));
			NUnit.Framework.Assert.AreEqual(501377302265856L, StringUtils.TraditionalBinaryPrefix.String2long
				("456t"));
			NUnit.Framework.Assert.AreEqual(-501377302265856L, StringUtils.TraditionalBinaryPrefix.String2long
				("-456T"));
			NUnit.Framework.Assert.AreEqual(11258999068426240L, StringUtils.TraditionalBinaryPrefix.String2long
				("10p"));
			NUnit.Framework.Assert.AreEqual(-11258999068426240L, StringUtils.TraditionalBinaryPrefix.String2long
				("-10P"));
			NUnit.Framework.Assert.AreEqual(1152921504606846976L, StringUtils.TraditionalBinaryPrefix.String2long
				("1e"));
			NUnit.Framework.Assert.AreEqual(-1152921504606846976L, StringUtils.TraditionalBinaryPrefix.String2long
				("-1E"));
			string tooLargeNumStr = "10e";
			try
			{
				StringUtils.TraditionalBinaryPrefix.String2long(tooLargeNumStr);
				NUnit.Framework.Assert.Fail("Test passed for a number " + tooLargeNumStr + " too large"
					);
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.AreEqual(tooLargeNumStr + " does not fit in a Long", e.Message
					);
			}
			string tooSmallNumStr = "-10e";
			try
			{
				StringUtils.TraditionalBinaryPrefix.String2long(tooSmallNumStr);
				NUnit.Framework.Assert.Fail("Test passed for a number " + tooSmallNumStr + " too small"
					);
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.AreEqual(tooSmallNumStr + " does not fit in a Long", e.Message
					);
			}
			string invalidFormatNumStr = "10kb";
			char invalidPrefix = 'b';
			try
			{
				StringUtils.TraditionalBinaryPrefix.String2long(invalidFormatNumStr);
				NUnit.Framework.Assert.Fail("Test passed for a number " + invalidFormatNumStr + " has invalid format"
					);
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.AreEqual("Invalid size prefix '" + invalidPrefix + "' in '"
					 + invalidFormatNumStr + "'. Allowed prefixes are k, m, g, t, p, e(case insensitive)"
					, e.Message);
			}
			//test long2string(..)
			NUnit.Framework.Assert.AreEqual("0", StringUtils.TraditionalBinaryPrefix.Long2String
				(0, null, 2));
			for (int decimalPlace = 0; decimalPlace < 2; decimalPlace++)
			{
				for (int n = 1; n < StringUtils.TraditionalBinaryPrefix.Kilo.value; n++)
				{
					NUnit.Framework.Assert.AreEqual(n + string.Empty, StringUtils.TraditionalBinaryPrefix.Long2String
						(n, null, decimalPlace));
					NUnit.Framework.Assert.AreEqual(-n + string.Empty, StringUtils.TraditionalBinaryPrefix.Long2String
						(-n, null, decimalPlace));
				}
				NUnit.Framework.Assert.AreEqual("1 K", StringUtils.TraditionalBinaryPrefix.Long2String
					(1L << 10, null, decimalPlace));
				NUnit.Framework.Assert.AreEqual("-1 K", StringUtils.TraditionalBinaryPrefix.Long2String
					(-1L << 10, null, decimalPlace));
			}
			NUnit.Framework.Assert.AreEqual("8.00 E", StringUtils.TraditionalBinaryPrefix.Long2String
				(long.MaxValue, null, 2));
			NUnit.Framework.Assert.AreEqual("8.00 E", StringUtils.TraditionalBinaryPrefix.Long2String
				(long.MaxValue - 1, null, 2));
			NUnit.Framework.Assert.AreEqual("-8 E", StringUtils.TraditionalBinaryPrefix.Long2String
				(long.MinValue, null, 2));
			NUnit.Framework.Assert.AreEqual("-8.00 E", StringUtils.TraditionalBinaryPrefix.Long2String
				(long.MinValue + 1, null, 2));
			string[] zeros = new string[] { " ", ".0 ", ".00 " };
			for (int decimalPlace_1 = 0; decimalPlace_1 < zeros.Length; decimalPlace_1++)
			{
				string trailingZeros = zeros[decimalPlace_1];
				for (int e = 11; e < long.Size - 1; e++)
				{
					StringUtils.TraditionalBinaryPrefix p = StringUtils.TraditionalBinaryPrefix.Values
						()[e / 10 - 1];
					{
						// n = 2^e
						long n = 1L << e;
						string expected = (n / p.value) + " " + p.symbol;
						NUnit.Framework.Assert.AreEqual("n=" + n, expected, StringUtils.TraditionalBinaryPrefix.Long2String
							(n, null, 2));
					}
					{
						// n = 2^e + 1
						long n = (1L << e) + 1;
						string expected = (n / p.value) + trailingZeros + p.symbol;
						NUnit.Framework.Assert.AreEqual("n=" + n, expected, StringUtils.TraditionalBinaryPrefix.Long2String
							(n, null, decimalPlace_1));
					}
					{
						// n = 2^e - 1
						long n = (1L << e) - 1;
						string expected = ((n + 1) / p.value) + trailingZeros + p.symbol;
						NUnit.Framework.Assert.AreEqual("n=" + n, expected, StringUtils.TraditionalBinaryPrefix.Long2String
							(n, null, decimalPlace_1));
					}
				}
			}
			NUnit.Framework.Assert.AreEqual("1.50 K", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 9, null, 2));
			NUnit.Framework.Assert.AreEqual("1.5 K", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 9, null, 1));
			NUnit.Framework.Assert.AreEqual("1.50 M", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 19, null, 2));
			NUnit.Framework.Assert.AreEqual("2 M", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 19, null, 0));
			NUnit.Framework.Assert.AreEqual("3 G", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 30, null, 2));
			// test byteDesc(..)
			NUnit.Framework.Assert.AreEqual("0 B", StringUtils.ByteDesc(0));
			NUnit.Framework.Assert.AreEqual("-100 B", StringUtils.ByteDesc(-100));
			NUnit.Framework.Assert.AreEqual("1 KB", StringUtils.ByteDesc(1024));
			NUnit.Framework.Assert.AreEqual("1.50 KB", StringUtils.ByteDesc(3L << 9));
			NUnit.Framework.Assert.AreEqual("1.50 MB", StringUtils.ByteDesc(3L << 19));
			NUnit.Framework.Assert.AreEqual("3 GB", StringUtils.ByteDesc(3L << 30));
			// test formatPercent(..)
			NUnit.Framework.Assert.AreEqual("10%", StringUtils.FormatPercent(0.1, 0));
			NUnit.Framework.Assert.AreEqual("10.0%", StringUtils.FormatPercent(0.1, 1));
			NUnit.Framework.Assert.AreEqual("10.00%", StringUtils.FormatPercent(0.1, 2));
			NUnit.Framework.Assert.AreEqual("1%", StringUtils.FormatPercent(0.00543, 0));
			NUnit.Framework.Assert.AreEqual("0.5%", StringUtils.FormatPercent(0.00543, 1));
			NUnit.Framework.Assert.AreEqual("0.54%", StringUtils.FormatPercent(0.00543, 2));
			NUnit.Framework.Assert.AreEqual("0.543%", StringUtils.FormatPercent(0.00543, 3));
			NUnit.Framework.Assert.AreEqual("0.5430%", StringUtils.FormatPercent(0.00543, 4));
		}

		public virtual void TestJoin()
		{
			IList<string> s = new AList<string>();
			s.AddItem("a");
			s.AddItem("b");
			s.AddItem("c");
			NUnit.Framework.Assert.AreEqual(string.Empty, StringUtils.Join(":", s.SubList(0, 
				0)));
			NUnit.Framework.Assert.AreEqual("a", StringUtils.Join(":", s.SubList(0, 1)));
			NUnit.Framework.Assert.AreEqual("a:b", StringUtils.Join(":", s.SubList(0, 2)));
			NUnit.Framework.Assert.AreEqual("a:b:c", StringUtils.Join(":", s.SubList(0, 3)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetTrimmedStrings()
		{
			string compactDirList = "/spindle1/hdfs,/spindle2/hdfs,/spindle3/hdfs";
			string spacedDirList = "/spindle1/hdfs, /spindle2/hdfs, /spindle3/hdfs";
			string pathologicalDirList1 = " /spindle1/hdfs  ,  /spindle2/hdfs ,/spindle3/hdfs ";
			string pathologicalDirList2 = " /spindle1/hdfs  ,  /spindle2/hdfs ,/spindle3/hdfs , ";
			string emptyList1 = string.Empty;
			string emptyList2 = "   ";
			string[] expectedArray = new string[] { "/spindle1/hdfs", "/spindle2/hdfs", "/spindle3/hdfs"
				 };
			string[] emptyArray = new string[] {  };
			Assert.AssertArrayEquals(expectedArray, StringUtils.GetTrimmedStrings(compactDirList
				));
			Assert.AssertArrayEquals(expectedArray, StringUtils.GetTrimmedStrings(spacedDirList
				));
			Assert.AssertArrayEquals(expectedArray, StringUtils.GetTrimmedStrings(pathologicalDirList1
				));
			Assert.AssertArrayEquals(expectedArray, StringUtils.GetTrimmedStrings(pathologicalDirList2
				));
			Assert.AssertArrayEquals(emptyArray, StringUtils.GetTrimmedStrings(emptyList1));
			string[] estring = StringUtils.GetTrimmedStrings(emptyList2);
			Assert.AssertArrayEquals(emptyArray, estring);
		}

		public virtual void TestCamelize()
		{
			// common use cases
			NUnit.Framework.Assert.AreEqual("Map", StringUtils.Camelize("MAP"));
			NUnit.Framework.Assert.AreEqual("JobSetup", StringUtils.Camelize("JOB_SETUP"));
			NUnit.Framework.Assert.AreEqual("SomeStuff", StringUtils.Camelize("some_stuff"));
			// sanity checks for ascii alphabet against unexpected locale issues.
			NUnit.Framework.Assert.AreEqual("Aa", StringUtils.Camelize("aA"));
			NUnit.Framework.Assert.AreEqual("Bb", StringUtils.Camelize("bB"));
			NUnit.Framework.Assert.AreEqual("Cc", StringUtils.Camelize("cC"));
			NUnit.Framework.Assert.AreEqual("Dd", StringUtils.Camelize("dD"));
			NUnit.Framework.Assert.AreEqual("Ee", StringUtils.Camelize("eE"));
			NUnit.Framework.Assert.AreEqual("Ff", StringUtils.Camelize("fF"));
			NUnit.Framework.Assert.AreEqual("Gg", StringUtils.Camelize("gG"));
			NUnit.Framework.Assert.AreEqual("Hh", StringUtils.Camelize("hH"));
			NUnit.Framework.Assert.AreEqual("Ii", StringUtils.Camelize("iI"));
			NUnit.Framework.Assert.AreEqual("Jj", StringUtils.Camelize("jJ"));
			NUnit.Framework.Assert.AreEqual("Kk", StringUtils.Camelize("kK"));
			NUnit.Framework.Assert.AreEqual("Ll", StringUtils.Camelize("lL"));
			NUnit.Framework.Assert.AreEqual("Mm", StringUtils.Camelize("mM"));
			NUnit.Framework.Assert.AreEqual("Nn", StringUtils.Camelize("nN"));
			NUnit.Framework.Assert.AreEqual("Oo", StringUtils.Camelize("oO"));
			NUnit.Framework.Assert.AreEqual("Pp", StringUtils.Camelize("pP"));
			NUnit.Framework.Assert.AreEqual("Qq", StringUtils.Camelize("qQ"));
			NUnit.Framework.Assert.AreEqual("Rr", StringUtils.Camelize("rR"));
			NUnit.Framework.Assert.AreEqual("Ss", StringUtils.Camelize("sS"));
			NUnit.Framework.Assert.AreEqual("Tt", StringUtils.Camelize("tT"));
			NUnit.Framework.Assert.AreEqual("Uu", StringUtils.Camelize("uU"));
			NUnit.Framework.Assert.AreEqual("Vv", StringUtils.Camelize("vV"));
			NUnit.Framework.Assert.AreEqual("Ww", StringUtils.Camelize("wW"));
			NUnit.Framework.Assert.AreEqual("Xx", StringUtils.Camelize("xX"));
			NUnit.Framework.Assert.AreEqual("Yy", StringUtils.Camelize("yY"));
			NUnit.Framework.Assert.AreEqual("Zz", StringUtils.Camelize("zZ"));
		}

		public virtual void TestStringToURI()
		{
			string[] str = new string[] { "file://" };
			try
			{
				StringUtils.StringToURI(str);
				NUnit.Framework.Assert.Fail("Ignoring URISyntaxException while creating URI from string file://"
					);
			}
			catch (ArgumentException iae)
			{
				NUnit.Framework.Assert.AreEqual("Failed to create uri for file://", iae.Message);
			}
		}

		public virtual void TestSimpleHostName()
		{
			NUnit.Framework.Assert.AreEqual("Should return hostname when FQDN is specified", 
				"hadoop01", StringUtils.SimpleHostname("hadoop01.domain.com"));
			NUnit.Framework.Assert.AreEqual("Should return hostname when only hostname is specified"
				, "hadoop01", StringUtils.SimpleHostname("hadoop01"));
			NUnit.Framework.Assert.AreEqual("Should not truncate when IP address is passed", 
				"10.10.5.68", StringUtils.SimpleHostname("10.10.5.68"));
		}

		public virtual void TestReplaceTokensShellEnvVars()
		{
			Sharpen.Pattern pattern = StringUtils.ShellEnvVarPattern;
			IDictionary<string, string> replacements = new Dictionary<string, string>();
			replacements["FOO"] = "one";
			replacements["BAZ"] = "two";
			replacements["NUMBERS123"] = "one-two-three";
			replacements["UNDER_SCORES"] = "___";
			NUnit.Framework.Assert.AreEqual("one", StringUtils.ReplaceTokens("$FOO", pattern, 
				replacements));
			NUnit.Framework.Assert.AreEqual("two", StringUtils.ReplaceTokens("$BAZ", pattern, 
				replacements));
			NUnit.Framework.Assert.AreEqual(string.Empty, StringUtils.ReplaceTokens("$BAR", pattern
				, replacements));
			NUnit.Framework.Assert.AreEqual(string.Empty, StringUtils.ReplaceTokens(string.Empty
				, pattern, replacements));
			NUnit.Framework.Assert.AreEqual("one-two-three", StringUtils.ReplaceTokens("$NUMBERS123"
				, pattern, replacements));
			NUnit.Framework.Assert.AreEqual("___", StringUtils.ReplaceTokens("$UNDER_SCORES", 
				pattern, replacements));
			NUnit.Framework.Assert.AreEqual("//one//two//", StringUtils.ReplaceTokens("//$FOO/$BAR/$BAZ//"
				, pattern, replacements));
		}

		public virtual void TestReplaceTokensWinEnvVars()
		{
			Sharpen.Pattern pattern = StringUtils.WinEnvVarPattern;
			IDictionary<string, string> replacements = new Dictionary<string, string>();
			replacements["foo"] = "zoo";
			replacements["baz"] = "zaz";
			NUnit.Framework.Assert.AreEqual("zoo", StringUtils.ReplaceTokens("%foo%", pattern
				, replacements));
			NUnit.Framework.Assert.AreEqual("zaz", StringUtils.ReplaceTokens("%baz%", pattern
				, replacements));
			NUnit.Framework.Assert.AreEqual(string.Empty, StringUtils.ReplaceTokens("%bar%", 
				pattern, replacements));
			NUnit.Framework.Assert.AreEqual(string.Empty, StringUtils.ReplaceTokens(string.Empty
				, pattern, replacements));
			NUnit.Framework.Assert.AreEqual("zoo__zaz", StringUtils.ReplaceTokens("%foo%_%bar%_%baz%"
				, pattern, replacements));
			NUnit.Framework.Assert.AreEqual("begin zoo__zaz end", StringUtils.ReplaceTokens("begin %foo%_%bar%_%baz% end"
				, pattern, replacements));
		}

		[NUnit.Framework.Test]
		public virtual void TestGetUniqueNonEmptyTrimmedStrings()
		{
			string ToSplit = ",foo, bar,baz,,blah,blah,bar,";
			ICollection<string> col = StringUtils.GetTrimmedStringCollection(ToSplit);
			NUnit.Framework.Assert.AreEqual(4, col.Count);
			NUnit.Framework.Assert.IsTrue(col.ContainsAll(Arrays.AsList(new string[] { "foo", 
				"bar", "baz", "blah" })));
		}

		[NUnit.Framework.Test]
		public virtual void TestLowerAndUpperStrings()
		{
			CultureInfo defaultLocale = CultureInfo.CurrentCulture;
			try
			{
				System.Threading.Thread.CurrentThread.CurrentCulture = new CultureInfo("tr", "TR"
					);
				string upperStr = "TITLE";
				string lowerStr = "title";
				// Confirming TR locale.
				Assert.AssertNotEquals(lowerStr, upperStr.ToLower());
				Assert.AssertNotEquals(upperStr, lowerStr.ToUpper());
				// This should be true regardless of locale.
				NUnit.Framework.Assert.AreEqual(lowerStr, StringUtils.ToLowerCase(upperStr));
				NUnit.Framework.Assert.AreEqual(upperStr, StringUtils.ToUpperCase(lowerStr));
				NUnit.Framework.Assert.IsTrue(StringUtils.EqualsIgnoreCase(upperStr, lowerStr));
			}
			finally
			{
				System.Threading.Thread.CurrentThread.CurrentCulture = defaultLocale;
			}
		}

		// Benchmark for StringUtils split
		public static void Main(string[] args)
		{
			string ToSplit = "foo,bar,baz,blah,blah";
			foreach (bool useOurs in new bool[] { false, true })
			{
				for (int outer = 0; outer < 10; outer++)
				{
					long st = Runtime.NanoTime();
					int components = 0;
					for (int inner = 0; inner < 1000000; inner++)
					{
						string[] res;
						if (useOurs)
						{
							res = StringUtils.Split(ToSplit, ',');
						}
						else
						{
							res = ToSplit.Split(",");
						}
						// be sure to use res, otherwise might be optimized out
						components += res.Length;
					}
					long et = Runtime.NanoTime();
					if (outer > 3)
					{
						System.Console.Out.WriteLine((useOurs ? "StringUtils impl" : "Java impl") + " #" 
							+ outer + ":" + (et - st) / 1000000 + "ms, components=" + components);
					}
				}
			}
		}
	}
}
