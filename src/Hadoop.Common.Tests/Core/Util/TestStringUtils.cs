using System;
using System.Collections.Generic;
using System.Globalization;
using NUnit.Framework;
using Org.Apache.Hadoop.Test;


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
			Assert.Equal(NullStr, StringUtils.EscapeString(NullStr));
			Assert.Equal(EmptyStr, StringUtils.EscapeString(EmptyStr));
			Assert.Equal(StrWoSpecialChars, StringUtils.EscapeString(StrWoSpecialChars
				));
			Assert.Equal(EscapedStrWithComma, StringUtils.EscapeString(StrWithComma
				));
			Assert.Equal(EscapedStrWithEscape, StringUtils.EscapeString(StrWithEscape
				));
			Assert.Equal(EscapedStrWithBoth2, StringUtils.EscapeString(StrWithBoth2
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSplit()
		{
			Assert.Equal(NullStr, StringUtils.Split(NullStr));
			string[] splits = StringUtils.Split(EmptyStr);
			Assert.Equal(0, splits.Length);
			splits = StringUtils.Split(",,");
			Assert.Equal(0, splits.Length);
			splits = StringUtils.Split(StrWoSpecialChars);
			Assert.Equal(1, splits.Length);
			Assert.Equal(StrWoSpecialChars, splits[0]);
			splits = StringUtils.Split(StrWithComma);
			Assert.Equal(2, splits.Length);
			Assert.Equal("A", splits[0]);
			Assert.Equal("B", splits[1]);
			splits = StringUtils.Split(EscapedStrWithComma);
			Assert.Equal(1, splits.Length);
			Assert.Equal(EscapedStrWithComma, splits[0]);
			splits = StringUtils.Split(StrWithEscape);
			Assert.Equal(1, splits.Length);
			Assert.Equal(StrWithEscape, splits[0]);
			splits = StringUtils.Split(StrWithBoth2);
			Assert.Equal(3, splits.Length);
			Assert.Equal(EmptyStr, splits[0]);
			Assert.Equal("A\\,", splits[1]);
			Assert.Equal("B\\\\", splits[2]);
			splits = StringUtils.Split(EscapedStrWithBoth2);
			Assert.Equal(1, splits.Length);
			Assert.Equal(EscapedStrWithBoth2, splits[0]);
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
			Assert.Equal(NullStr, StringUtils.UnEscapeString(NullStr));
			Assert.Equal(EmptyStr, StringUtils.UnEscapeString(EmptyStr));
			Assert.Equal(StrWoSpecialChars, StringUtils.UnEscapeString(StrWoSpecialChars
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
			Assert.Equal(StrWithComma, StringUtils.UnEscapeString(EscapedStrWithComma
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
			Assert.Equal(StrWithEscape, StringUtils.UnEscapeString(EscapedStrWithEscape
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
			Assert.Equal(StrWithBoth2, StringUtils.UnEscapeString(EscapedStrWithBoth2
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
				Assert.Equal(0, StringUtils.TraditionalBinaryPrefix.String2long
					(0 + s));
				Assert.Equal(m, StringUtils.TraditionalBinaryPrefix.String2long
					(1 + s));
				m *= 1024;
			}
			Assert.Equal(0L, StringUtils.TraditionalBinaryPrefix.String2long
				("0"));
			Assert.Equal(1024L, StringUtils.TraditionalBinaryPrefix.String2long
				("1k"));
			Assert.Equal(-1024L, StringUtils.TraditionalBinaryPrefix.String2long
				("-1k"));
			Assert.Equal(1259520L, StringUtils.TraditionalBinaryPrefix.String2long
				("1230K"));
			Assert.Equal(-1259520L, StringUtils.TraditionalBinaryPrefix.String2long
				("-1230K"));
			Assert.Equal(104857600L, StringUtils.TraditionalBinaryPrefix.String2long
				("100m"));
			Assert.Equal(-104857600L, StringUtils.TraditionalBinaryPrefix.String2long
				("-100M"));
			Assert.Equal(956703965184L, StringUtils.TraditionalBinaryPrefix.String2long
				("891g"));
			Assert.Equal(-956703965184L, StringUtils.TraditionalBinaryPrefix.String2long
				("-891G"));
			Assert.Equal(501377302265856L, StringUtils.TraditionalBinaryPrefix.String2long
				("456t"));
			Assert.Equal(-501377302265856L, StringUtils.TraditionalBinaryPrefix.String2long
				("-456T"));
			Assert.Equal(11258999068426240L, StringUtils.TraditionalBinaryPrefix.String2long
				("10p"));
			Assert.Equal(-11258999068426240L, StringUtils.TraditionalBinaryPrefix.String2long
				("-10P"));
			Assert.Equal(1152921504606846976L, StringUtils.TraditionalBinaryPrefix.String2long
				("1e"));
			Assert.Equal(-1152921504606846976L, StringUtils.TraditionalBinaryPrefix.String2long
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
				Assert.Equal(tooLargeNumStr + " does not fit in a Long", e.Message
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
				Assert.Equal(tooSmallNumStr + " does not fit in a Long", e.Message
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
				Assert.Equal("Invalid size prefix '" + invalidPrefix + "' in '"
					 + invalidFormatNumStr + "'. Allowed prefixes are k, m, g, t, p, e(case insensitive)"
					, e.Message);
			}
			//test long2string(..)
			Assert.Equal("0", StringUtils.TraditionalBinaryPrefix.Long2String
				(0, null, 2));
			for (int decimalPlace = 0; decimalPlace < 2; decimalPlace++)
			{
				for (int n = 1; n < StringUtils.TraditionalBinaryPrefix.Kilo.value; n++)
				{
					Assert.Equal(n + string.Empty, StringUtils.TraditionalBinaryPrefix.Long2String
						(n, null, decimalPlace));
					Assert.Equal(-n + string.Empty, StringUtils.TraditionalBinaryPrefix.Long2String
						(-n, null, decimalPlace));
				}
				Assert.Equal("1 K", StringUtils.TraditionalBinaryPrefix.Long2String
					(1L << 10, null, decimalPlace));
				Assert.Equal("-1 K", StringUtils.TraditionalBinaryPrefix.Long2String
					(-1L << 10, null, decimalPlace));
			}
			Assert.Equal("8.00 E", StringUtils.TraditionalBinaryPrefix.Long2String
				(long.MaxValue, null, 2));
			Assert.Equal("8.00 E", StringUtils.TraditionalBinaryPrefix.Long2String
				(long.MaxValue - 1, null, 2));
			Assert.Equal("-8 E", StringUtils.TraditionalBinaryPrefix.Long2String
				(long.MinValue, null, 2));
			Assert.Equal("-8.00 E", StringUtils.TraditionalBinaryPrefix.Long2String
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
						Assert.Equal("n=" + n, expected, StringUtils.TraditionalBinaryPrefix.Long2String
							(n, null, 2));
					}
					{
						// n = 2^e + 1
						long n = (1L << e) + 1;
						string expected = (n / p.value) + trailingZeros + p.symbol;
						Assert.Equal("n=" + n, expected, StringUtils.TraditionalBinaryPrefix.Long2String
							(n, null, decimalPlace_1));
					}
					{
						// n = 2^e - 1
						long n = (1L << e) - 1;
						string expected = ((n + 1) / p.value) + trailingZeros + p.symbol;
						Assert.Equal("n=" + n, expected, StringUtils.TraditionalBinaryPrefix.Long2String
							(n, null, decimalPlace_1));
					}
				}
			}
			Assert.Equal("1.50 K", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 9, null, 2));
			Assert.Equal("1.5 K", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 9, null, 1));
			Assert.Equal("1.50 M", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 19, null, 2));
			Assert.Equal("2 M", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 19, null, 0));
			Assert.Equal("3 G", StringUtils.TraditionalBinaryPrefix.Long2String
				(3L << 30, null, 2));
			// test byteDesc(..)
			Assert.Equal("0 B", StringUtils.ByteDesc(0));
			Assert.Equal("-100 B", StringUtils.ByteDesc(-100));
			Assert.Equal("1 KB", StringUtils.ByteDesc(1024));
			Assert.Equal("1.50 KB", StringUtils.ByteDesc(3L << 9));
			Assert.Equal("1.50 MB", StringUtils.ByteDesc(3L << 19));
			Assert.Equal("3 GB", StringUtils.ByteDesc(3L << 30));
			// test formatPercent(..)
			Assert.Equal("10%", StringUtils.FormatPercent(0.1, 0));
			Assert.Equal("10.0%", StringUtils.FormatPercent(0.1, 1));
			Assert.Equal("10.00%", StringUtils.FormatPercent(0.1, 2));
			Assert.Equal("1%", StringUtils.FormatPercent(0.00543, 0));
			Assert.Equal("0.5%", StringUtils.FormatPercent(0.00543, 1));
			Assert.Equal("0.54%", StringUtils.FormatPercent(0.00543, 2));
			Assert.Equal("0.543%", StringUtils.FormatPercent(0.00543, 3));
			Assert.Equal("0.5430%", StringUtils.FormatPercent(0.00543, 4));
		}

		public virtual void TestJoin()
		{
			IList<string> s = new AList<string>();
			s.AddItem("a");
			s.AddItem("b");
			s.AddItem("c");
			Assert.Equal(string.Empty, StringUtils.Join(":", s.SubList(0, 
				0)));
			Assert.Equal("a", StringUtils.Join(":", s.SubList(0, 1)));
			Assert.Equal("a:b", StringUtils.Join(":", s.SubList(0, 2)));
			Assert.Equal("a:b:c", StringUtils.Join(":", s.SubList(0, 3)));
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
			Assert.Equal("Map", StringUtils.Camelize("MAP"));
			Assert.Equal("JobSetup", StringUtils.Camelize("JOB_SETUP"));
			Assert.Equal("SomeStuff", StringUtils.Camelize("some_stuff"));
			// sanity checks for ascii alphabet against unexpected locale issues.
			Assert.Equal("Aa", StringUtils.Camelize("aA"));
			Assert.Equal("Bb", StringUtils.Camelize("bB"));
			Assert.Equal("Cc", StringUtils.Camelize("cC"));
			Assert.Equal("Dd", StringUtils.Camelize("dD"));
			Assert.Equal("Ee", StringUtils.Camelize("eE"));
			Assert.Equal("Ff", StringUtils.Camelize("fF"));
			Assert.Equal("Gg", StringUtils.Camelize("gG"));
			Assert.Equal("Hh", StringUtils.Camelize("hH"));
			Assert.Equal("Ii", StringUtils.Camelize("iI"));
			Assert.Equal("Jj", StringUtils.Camelize("jJ"));
			Assert.Equal("Kk", StringUtils.Camelize("kK"));
			Assert.Equal("Ll", StringUtils.Camelize("lL"));
			Assert.Equal("Mm", StringUtils.Camelize("mM"));
			Assert.Equal("Nn", StringUtils.Camelize("nN"));
			Assert.Equal("Oo", StringUtils.Camelize("oO"));
			Assert.Equal("Pp", StringUtils.Camelize("pP"));
			Assert.Equal("Qq", StringUtils.Camelize("qQ"));
			Assert.Equal("Rr", StringUtils.Camelize("rR"));
			Assert.Equal("Ss", StringUtils.Camelize("sS"));
			Assert.Equal("Tt", StringUtils.Camelize("tT"));
			Assert.Equal("Uu", StringUtils.Camelize("uU"));
			Assert.Equal("Vv", StringUtils.Camelize("vV"));
			Assert.Equal("Ww", StringUtils.Camelize("wW"));
			Assert.Equal("Xx", StringUtils.Camelize("xX"));
			Assert.Equal("Yy", StringUtils.Camelize("yY"));
			Assert.Equal("Zz", StringUtils.Camelize("zZ"));
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
				Assert.Equal("Failed to create uri for file://", iae.Message);
			}
		}

		public virtual void TestSimpleHostName()
		{
			Assert.Equal("Should return hostname when FQDN is specified", 
				"hadoop01", StringUtils.SimpleHostname("hadoop01.domain.com"));
			Assert.Equal("Should return hostname when only hostname is specified"
				, "hadoop01", StringUtils.SimpleHostname("hadoop01"));
			Assert.Equal("Should not truncate when IP address is passed", 
				"10.10.5.68", StringUtils.SimpleHostname("10.10.5.68"));
		}

		public virtual void TestReplaceTokensShellEnvVars()
		{
			Pattern pattern = StringUtils.ShellEnvVarPattern;
			IDictionary<string, string> replacements = new Dictionary<string, string>();
			replacements["FOO"] = "one";
			replacements["BAZ"] = "two";
			replacements["NUMBERS123"] = "one-two-three";
			replacements["UNDER_SCORES"] = "___";
			Assert.Equal("one", StringUtils.ReplaceTokens("$FOO", pattern, 
				replacements));
			Assert.Equal("two", StringUtils.ReplaceTokens("$BAZ", pattern, 
				replacements));
			Assert.Equal(string.Empty, StringUtils.ReplaceTokens("$BAR", pattern
				, replacements));
			Assert.Equal(string.Empty, StringUtils.ReplaceTokens(string.Empty
				, pattern, replacements));
			Assert.Equal("one-two-three", StringUtils.ReplaceTokens("$NUMBERS123"
				, pattern, replacements));
			Assert.Equal("___", StringUtils.ReplaceTokens("$UNDER_SCORES", 
				pattern, replacements));
			Assert.Equal("//one//two//", StringUtils.ReplaceTokens("//$FOO/$BAR/$BAZ//"
				, pattern, replacements));
		}

		public virtual void TestReplaceTokensWinEnvVars()
		{
			Pattern pattern = StringUtils.WinEnvVarPattern;
			IDictionary<string, string> replacements = new Dictionary<string, string>();
			replacements["foo"] = "zoo";
			replacements["baz"] = "zaz";
			Assert.Equal("zoo", StringUtils.ReplaceTokens("%foo%", pattern
				, replacements));
			Assert.Equal("zaz", StringUtils.ReplaceTokens("%baz%", pattern
				, replacements));
			Assert.Equal(string.Empty, StringUtils.ReplaceTokens("%bar%", 
				pattern, replacements));
			Assert.Equal(string.Empty, StringUtils.ReplaceTokens(string.Empty
				, pattern, replacements));
			Assert.Equal("zoo__zaz", StringUtils.ReplaceTokens("%foo%_%bar%_%baz%"
				, pattern, replacements));
			Assert.Equal("begin zoo__zaz end", StringUtils.ReplaceTokens("begin %foo%_%bar%_%baz% end"
				, pattern, replacements));
		}

		[Fact]
		public virtual void TestGetUniqueNonEmptyTrimmedStrings()
		{
			string ToSplit = ",foo, bar,baz,,blah,blah,bar,";
			ICollection<string> col = StringUtils.GetTrimmedStringCollection(ToSplit);
			Assert.Equal(4, col.Count);
			Assert.True(col.ContainsAll(Arrays.AsList(new string[] { "foo", 
				"bar", "baz", "blah" })));
		}

		[Fact]
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
				Assert.Equal(lowerStr, StringUtils.ToLowerCase(upperStr));
				Assert.Equal(upperStr, StringUtils.ToUpperCase(lowerStr));
				Assert.True(StringUtils.EqualsIgnoreCase(upperStr, lowerStr));
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
