using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestVersionUtil
	{
		[NUnit.Framework.Test]
		public virtual void TestCompareVersions()
		{
			// Equal versions are equal.
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("2.0.0", "2.0.0"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("2.0.0a", "2.0.0a"
				));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("2.0.0-SNAPSHOT", 
				"2.0.0-SNAPSHOT"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1", "1"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1", "1.0"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1", "1.0.0"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.0", "1"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.0", "1.0"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.0", "1.0.0"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.0.0", "1"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.0.0", "1.0"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.0.0", "1.0.0"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.0.0-alpha-1", "1.0.0-a1"
				));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.0.0-alpha-2", "1.0.0-a2"
				));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.0.0-alpha1", "1.0.0-alpha-1"
				));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1a0", "1.0.0-alpha-0"
				));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1a0", "1-a0"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.a0", "1-a0"));
			NUnit.Framework.Assert.AreEqual(0, VersionUtil.CompareVersions("1.a0", "1.0.0-alpha-0"
				));
			// Assert that lower versions are lower, and higher versions are higher.
			AssertExpectedValues("1", "2.0.0");
			AssertExpectedValues("1.0.0", "2");
			AssertExpectedValues("1.0.0", "2.0.0");
			AssertExpectedValues("1.0", "2.0.0");
			AssertExpectedValues("1.0.0", "2.0.0");
			AssertExpectedValues("1.0.0", "1.0.0a");
			AssertExpectedValues("1.0.0.0", "2.0.0");
			AssertExpectedValues("1.0.0", "1.0.0-dev");
			AssertExpectedValues("1.0.0", "1.0.1");
			AssertExpectedValues("1.0.0", "1.0.2");
			AssertExpectedValues("1.0.0", "1.1.0");
			AssertExpectedValues("2.0.0", "10.0.0");
			AssertExpectedValues("1.0.0", "1.0.0a");
			AssertExpectedValues("1.0.2a", "1.0.10");
			AssertExpectedValues("1.0.2a", "1.0.2b");
			AssertExpectedValues("1.0.2a", "1.0.2ab");
			AssertExpectedValues("1.0.0a1", "1.0.0a2");
			AssertExpectedValues("1.0.0a2", "1.0.0a10");
			// The 'a' in "1.a" is not followed by digit, thus not treated as "alpha",
			// and treated larger than "1.0", per maven's ComparableVersion class
			// implementation.
			AssertExpectedValues("1.0", "1.a");
			//The 'a' in "1.a0" is followed by digit, thus treated as "alpha-<digit>"
			AssertExpectedValues("1.a0", "1.0");
			AssertExpectedValues("1a0", "1.0");
			AssertExpectedValues("1.0.1-alpha-1", "1.0.1-alpha-2");
			AssertExpectedValues("1.0.1-beta-1", "1.0.1-beta-2");
			// Snapshot builds precede their eventual releases.
			AssertExpectedValues("1.0-SNAPSHOT", "1.0");
			AssertExpectedValues("1.0.0-SNAPSHOT", "1.0");
			AssertExpectedValues("1.0.0-SNAPSHOT", "1.0.0");
			AssertExpectedValues("1.0.0", "1.0.1-SNAPSHOT");
			AssertExpectedValues("1.0.1-SNAPSHOT", "1.0.1");
			AssertExpectedValues("1.0.1-SNAPSHOT", "1.0.2");
			AssertExpectedValues("1.0.1-alpha-1", "1.0.1-SNAPSHOT");
			AssertExpectedValues("1.0.1-beta-1", "1.0.1-SNAPSHOT");
			AssertExpectedValues("1.0.1-beta-2", "1.0.1-SNAPSHOT");
		}

		private static void AssertExpectedValues(string lower, string higher)
		{
			NUnit.Framework.Assert.IsTrue(VersionUtil.CompareVersions(lower, higher) < 0);
			NUnit.Framework.Assert.IsTrue(VersionUtil.CompareVersions(higher, lower) > 0);
		}
	}
}
