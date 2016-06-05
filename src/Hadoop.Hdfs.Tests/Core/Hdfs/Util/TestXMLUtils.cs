using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	public class TestXMLUtils
	{
		private static void TestRoundTripImpl(string str, string expectedMangled, bool encodeEntityRefs
			)
		{
			string mangled = XMLUtils.MangleXmlString(str, encodeEntityRefs);
			NUnit.Framework.Assert.AreEqual(expectedMangled, mangled);
			string unmangled = XMLUtils.UnmangleXmlString(mangled, encodeEntityRefs);
			NUnit.Framework.Assert.AreEqual(str, unmangled);
		}

		private static void TestRoundTrip(string str, string expectedMangled)
		{
			TestRoundTripImpl(str, expectedMangled, false);
		}

		private static void TestRoundTripWithEntityRefs(string str, string expectedMangled
			)
		{
			TestRoundTripImpl(str, expectedMangled, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMangleEmptyString()
		{
			TestRoundTrip(string.Empty, string.Empty);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMangleVanillaString()
		{
			TestRoundTrip("abcdef", "abcdef");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMangleStringWithBackSlash()
		{
			TestRoundTrip("a\\bcdef", "a\\005c;bcdef");
			TestRoundTrip("\\\\", "\\005c;\\005c;");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMangleStringWithForbiddenCodePoint()
		{
			TestRoundTrip("a\u0001bcdef", "a\\0001;bcdef");
			TestRoundTrip("a\u0002\ud800bcdef", "a\\0002;\\d800;bcdef");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidSequence()
		{
			try
			{
				XMLUtils.UnmangleXmlString("\\000g;foo", false);
				NUnit.Framework.Assert.Fail("expected an unmangling error");
			}
			catch (XMLUtils.UnmanglingError)
			{
			}
			// pass through
			try
			{
				XMLUtils.UnmangleXmlString("\\0", false);
				NUnit.Framework.Assert.Fail("expected an unmangling error");
			}
			catch (XMLUtils.UnmanglingError)
			{
			}
		}

		// pass through
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddEntityRefs()
		{
			TestRoundTripWithEntityRefs("The Itchy & Scratchy Show", "The Itchy &amp; Scratchy Show"
				);
			TestRoundTripWithEntityRefs("\"He said '1 < 2, but 2 > 1'\"", "&quot;He said &apos;1 &lt; 2, but 2 &gt; 1&apos;&quot;"
				);
			TestRoundTripWithEntityRefs("\u0001 < \u0002", "\\0001; &lt; \\0002;");
		}
	}
}
