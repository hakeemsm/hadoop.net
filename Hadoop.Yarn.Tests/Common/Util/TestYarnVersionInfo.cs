using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>
	/// A JUnit test to test
	/// <see cref="YarnVersionInfo"/>
	/// </summary>
	public class TestYarnVersionInfo
	{
		/// <summary>Test the yarn version info routines.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void VersionInfoGenerated()
		{
			// can't easily know what the correct values are going to be so just
			// make sure they aren't Unknown
			NUnit.Framework.Assert.IsTrue("getVersion returned Unknown", !YarnVersionInfo.GetVersion
				().Equals("Unknown"));
			NUnit.Framework.Assert.IsTrue("getUser returned Unknown", !YarnVersionInfo.GetUser
				().Equals("Unknown"));
			NUnit.Framework.Assert.IsTrue("getUrl returned Unknown", !YarnVersionInfo.GetUrl(
				).Equals("Unknown"));
			NUnit.Framework.Assert.IsTrue("getSrcChecksum returned Unknown", !YarnVersionInfo
				.GetSrcChecksum().Equals("Unknown"));
			// these could be Unknown if the VersionInfo generated from code not in svn or git
			// so just check that they return something
			NUnit.Framework.Assert.IsNotNull("getRevision returned null", YarnVersionInfo.GetRevision
				());
			NUnit.Framework.Assert.IsNotNull("getBranch returned null", YarnVersionInfo.GetBranch
				());
			NUnit.Framework.Assert.IsTrue("getBuildVersion check doesn't contain: source checksum"
				, YarnVersionInfo.GetBuildVersion().Contains("source checksum"));
		}
	}
}
