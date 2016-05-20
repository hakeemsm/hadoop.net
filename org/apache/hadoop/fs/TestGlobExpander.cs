using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestGlobExpander : NUnit.Framework.TestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual void testExpansionIsIdentical()
		{
			checkExpansionIsIdentical(string.Empty);
			checkExpansionIsIdentical("/}");
			checkExpansionIsIdentical("/}{a,b}");
			checkExpansionIsIdentical("{/");
			checkExpansionIsIdentical("{a}");
			checkExpansionIsIdentical("{a,b}/{b,c}");
			checkExpansionIsIdentical("p\\{a/b,c/d\\}s");
			checkExpansionIsIdentical("p{a\\/b,c\\/d}s");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testExpansion()
		{
			checkExpansion("{a/b}", "a/b");
			checkExpansion("/}{a/b}", "/}a/b");
			checkExpansion("p{a/b,c/d}s", "pa/bs", "pc/ds");
			checkExpansion("{a/b,c/d,{e,f}}", "a/b", "c/d", "{e,f}");
			checkExpansion("{a/b,c/d}{e,f}", "a/b{e,f}", "c/d{e,f}");
			checkExpansion("{a,b}/{b,{c/d,e/f}}", "{a,b}/b", "{a,b}/c/d", "{a,b}/e/f");
			checkExpansion("{a,b}/{c/\\d}", "{a,b}/c/d");
		}

		/// <exception cref="System.IO.IOException"/>
		private void checkExpansionIsIdentical(string filePattern)
		{
			checkExpansion(filePattern, filePattern);
		}

		/// <exception cref="System.IO.IOException"/>
		private void checkExpansion(string filePattern, params string[] expectedExpansions
			)
		{
			System.Collections.Generic.IList<string> actualExpansions = org.apache.hadoop.fs.GlobExpander
				.expand(filePattern);
			NUnit.Framework.Assert.AreEqual("Different number of expansions", expectedExpansions
				.Length, actualExpansions.Count);
			for (int i = 0; i < expectedExpansions.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual("Expansion of " + filePattern, expectedExpansions
					[i], actualExpansions[i]);
			}
		}
	}
}
