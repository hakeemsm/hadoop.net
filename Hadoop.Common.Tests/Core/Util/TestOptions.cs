using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestOptions
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppend()
		{
			Assert.AssertArrayEquals("first append", new string[] { "Dr.", "Who", "hi", "there"
				 }, Options.PrependOptions(new string[] { "hi", "there" }, "Dr.", "Who"));
			Assert.AssertArrayEquals("second append", new string[] { "aa", "bb", "cc", "dd", 
				"ee", "ff" }, Options.PrependOptions(new string[] { "dd", "ee", "ff" }, "aa", "bb"
				, "cc"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFind()
		{
			object[] opts = new object[] { 1, "hi", true, "bye", 'x' };
			NUnit.Framework.Assert.AreEqual(1, Options.GetOption<int>(opts));
			NUnit.Framework.Assert.AreEqual("hi", Options.GetOption<string>(opts));
			NUnit.Framework.Assert.AreEqual(true, Options.GetOption<bool>(opts));
		}
	}
}
