using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// Test the ViewFsBaseTest using a viewfs with authority:
	/// viewfs://mountTableName/
	/// ie the authority is used to load a mount table.
	/// </summary>
	/// <remarks>
	/// Test the ViewFsBaseTest using a viewfs with authority:
	/// viewfs://mountTableName/
	/// ie the authority is used to load a mount table.
	/// The authority name used is "default"
	/// </remarks>
	public class TestViewFileSystemWithAuthorityLocalFileSystem : org.apache.hadoop.fs.viewfs.ViewFileSystemBaseTest
	{
		internal java.net.URI schemeWithAuthority;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			// create the test root on local_fs
			fsTarget = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration
				());
			base.setUp();
			// this sets up conf (and fcView which we replace)
			// Now create a viewfs using a mount table called "default"
			// hence viewfs://default/
			schemeWithAuthority = new java.net.URI(org.apache.hadoop.fs.FsConstants.VIEWFS_SCHEME
				, "default", "/", null, null);
			fsView = org.apache.hadoop.fs.FileSystem.get(schemeWithAuthority, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void tearDown()
		{
			fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
			base.tearDown();
		}

		[NUnit.Framework.Test]
		public override void testBasicPaths()
		{
			NUnit.Framework.Assert.AreEqual(schemeWithAuthority, fsView.getUri());
			NUnit.Framework.Assert.AreEqual(fsView.makeQualified(new org.apache.hadoop.fs.Path
				("/user/" + Sharpen.Runtime.getProperty("user.name"))), fsView.getWorkingDirectory
				());
			NUnit.Framework.Assert.AreEqual(fsView.makeQualified(new org.apache.hadoop.fs.Path
				("/user/" + Sharpen.Runtime.getProperty("user.name"))), fsView.getHomeDirectory(
				));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("/foo/bar").makeQualified
				(schemeWithAuthority, null), fsView.makeQualified(new org.apache.hadoop.fs.Path(
				"/foo/bar")));
		}
	}
}
