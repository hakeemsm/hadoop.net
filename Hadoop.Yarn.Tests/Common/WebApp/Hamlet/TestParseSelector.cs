using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Hamlet
{
	public class TestParseSelector
	{
		[NUnit.Framework.Test]
		public virtual void TestNormal()
		{
			string[] res = HamletImpl.ParseSelector("#id.class");
			NUnit.Framework.Assert.AreEqual("id", res[SId]);
			NUnit.Framework.Assert.AreEqual("class", res[SClass]);
		}

		[NUnit.Framework.Test]
		public virtual void TestMultiClass()
		{
			string[] res = HamletImpl.ParseSelector("#id.class1.class2");
			NUnit.Framework.Assert.AreEqual("id", res[SId]);
			NUnit.Framework.Assert.AreEqual("class1 class2", res[SClass]);
		}

		[NUnit.Framework.Test]
		public virtual void TestMissingId()
		{
			string[] res = HamletImpl.ParseSelector(".class");
			NUnit.Framework.Assert.IsNull(res[SId]);
			NUnit.Framework.Assert.AreEqual("class", res[SClass]);
		}

		[NUnit.Framework.Test]
		public virtual void TestMissingClass()
		{
			string[] res = HamletImpl.ParseSelector("#id");
			NUnit.Framework.Assert.AreEqual("id", res[SId]);
			NUnit.Framework.Assert.IsNull(res[SClass]);
		}

		public virtual void TestMissingAll()
		{
			HamletImpl.ParseSelector(string.Empty);
		}
	}
}
