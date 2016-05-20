using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Tests for glob patterns</summary>
	public class TestGlobPattern
	{
		private void assertMatch(bool yes, string glob, params string[] input)
		{
			org.apache.hadoop.fs.GlobPattern pattern = new org.apache.hadoop.fs.GlobPattern(glob
				);
			foreach (string s in input)
			{
				bool result = pattern.matches(s);
				NUnit.Framework.Assert.IsTrue(glob + " should" + (yes ? string.Empty : " not") + 
					" match " + s, yes ? result : !result);
			}
		}

		private void shouldThrow(params string[] globs)
		{
			foreach (string glob in globs)
			{
				try
				{
					org.apache.hadoop.fs.GlobPattern.compile(glob);
				}
				catch (java.util.regex.PatternSyntaxException e)
				{
					Sharpen.Runtime.printStackTrace(e);
					continue;
				}
				NUnit.Framework.Assert.IsTrue("glob " + glob + " should throw", false);
			}
		}

		[NUnit.Framework.Test]
		public virtual void testValidPatterns()
		{
			assertMatch(true, "*", "^$", "foo", "bar");
			assertMatch(true, "?", "?", "^", "[", "]", "$");
			assertMatch(true, "foo*", "foo", "food", "fool");
			assertMatch(true, "f*d", "fud", "food");
			assertMatch(true, "*d", "good", "bad");
			assertMatch(true, "\\*\\?\\[\\{\\\\", "*?[{\\");
			assertMatch(true, "[]^-]", "]", "-", "^");
			assertMatch(true, "]", "]");
			assertMatch(true, "^.$()|+", "^.$()|+");
			assertMatch(true, "[^^]", ".", "$", "[", "]");
			assertMatch(false, "[^^]", "^");
			assertMatch(true, "[!!-]", "^", "?");
			assertMatch(false, "[!!-]", "!", "-");
			assertMatch(true, "{[12]*,[45]*,[78]*}", "1", "2!", "4", "42", "7", "7$");
			assertMatch(false, "{[12]*,[45]*,[78]*}", "3", "6", "9ß");
			assertMatch(true, "}", "}");
		}

		[NUnit.Framework.Test]
		public virtual void testInvalidPatterns()
		{
			shouldThrow("[", "[[]]", "[][]", "{", "\\");
		}
	}
}
