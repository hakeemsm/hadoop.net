using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestGlobExpander : TestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestExpansionIsIdentical()
		{
			CheckExpansionIsIdentical(string.Empty);
			CheckExpansionIsIdentical("/}");
			CheckExpansionIsIdentical("/}{a,b}");
			CheckExpansionIsIdentical("{/");
			CheckExpansionIsIdentical("{a}");
			CheckExpansionIsIdentical("{a,b}/{b,c}");
			CheckExpansionIsIdentical("p\\{a/b,c/d\\}s");
			CheckExpansionIsIdentical("p{a\\/b,c\\/d}s");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestExpansion()
		{
			CheckExpansion("{a/b}", "a/b");
			CheckExpansion("/}{a/b}", "/}a/b");
			CheckExpansion("p{a/b,c/d}s", "pa/bs", "pc/ds");
			CheckExpansion("{a/b,c/d,{e,f}}", "a/b", "c/d", "{e,f}");
			CheckExpansion("{a/b,c/d}{e,f}", "a/b{e,f}", "c/d{e,f}");
			CheckExpansion("{a,b}/{b,{c/d,e/f}}", "{a,b}/b", "{a,b}/c/d", "{a,b}/e/f");
			CheckExpansion("{a,b}/{c/\\d}", "{a,b}/c/d");
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckExpansionIsIdentical(string filePattern)
		{
			CheckExpansion(filePattern, filePattern);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckExpansion(string filePattern, params string[] expectedExpansions
			)
		{
			IList<string> actualExpansions = GlobExpander.Expand(filePattern);
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
