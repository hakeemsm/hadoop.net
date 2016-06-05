using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	public class TestParseRoute
	{
		[NUnit.Framework.Test]
		public virtual void TestNormalAction()
		{
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/foo/action", "foo", "action", ":a1"
				, ":a2"), WebApp.ParseRoute("/foo/action/:a1/:a2"));
		}

		[NUnit.Framework.Test]
		public virtual void TestDefaultController()
		{
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/", "default", "index"), WebApp.ParseRoute
				("/"));
		}

		[NUnit.Framework.Test]
		public virtual void TestDefaultAction()
		{
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/foo", "foo", "index"), WebApp.ParseRoute
				("/foo"));
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/foo", "foo", "index"), WebApp.ParseRoute
				("/foo/"));
		}

		[NUnit.Framework.Test]
		public virtual void TestMissingAction()
		{
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/foo", "foo", "index", ":a1"), WebApp
				.ParseRoute("/foo/:a1"));
		}

		[NUnit.Framework.Test]
		public virtual void TestDefaultCapture()
		{
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/", "default", "index", ":a"), WebApp
				.ParseRoute("/:a"));
		}

		[NUnit.Framework.Test]
		public virtual void TestPartialCapture1()
		{
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/foo/action/bar", "foo", "action", 
				"bar", ":a"), WebApp.ParseRoute("/foo/action/bar/:a"));
		}

		[NUnit.Framework.Test]
		public virtual void TestPartialCapture2()
		{
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/foo/action", "foo", "action", ":a1"
				, "bar", ":a2", ":a3"), WebApp.ParseRoute("/foo/action/:a1/bar/:a2/:a3"));
		}

		[NUnit.Framework.Test]
		public virtual void TestLeadingPaddings()
		{
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/foo/action", "foo", "action", ":a"
				), WebApp.ParseRoute(" /foo/action/ :a"));
		}

		[NUnit.Framework.Test]
		public virtual void TestTrailingPaddings()
		{
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/foo/action", "foo", "action", ":a"
				), WebApp.ParseRoute("/foo/action//:a / "));
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("/foo/action", "foo", "action"), WebApp
				.ParseRoute("/foo/action / "));
		}

		public virtual void TestMissingLeadingSlash()
		{
			WebApp.ParseRoute("foo/bar");
		}
	}
}
