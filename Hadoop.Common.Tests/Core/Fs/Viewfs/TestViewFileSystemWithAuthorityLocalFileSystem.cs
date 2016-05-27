using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
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
	public class TestViewFileSystemWithAuthorityLocalFileSystem : ViewFileSystemBaseTest
	{
		internal URI schemeWithAuthority;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			// create the test root on local_fs
			fsTarget = FileSystem.GetLocal(new Configuration());
			base.SetUp();
			// this sets up conf (and fcView which we replace)
			// Now create a viewfs using a mount table called "default"
			// hence viewfs://default/
			schemeWithAuthority = new URI(FsConstants.ViewfsScheme, "default", "/", null, null
				);
			fsView = FileSystem.Get(schemeWithAuthority, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			fsTarget.Delete(fileSystemTestHelper.GetTestRootPath(fsTarget), true);
			base.TearDown();
		}

		[NUnit.Framework.Test]
		public override void TestBasicPaths()
		{
			NUnit.Framework.Assert.AreEqual(schemeWithAuthority, fsView.GetUri());
			NUnit.Framework.Assert.AreEqual(fsView.MakeQualified(new Path("/user/" + Runtime.
				GetProperty("user.name"))), fsView.GetWorkingDirectory());
			NUnit.Framework.Assert.AreEqual(fsView.MakeQualified(new Path("/user/" + Runtime.
				GetProperty("user.name"))), fsView.GetHomeDirectory());
			NUnit.Framework.Assert.AreEqual(new Path("/foo/bar").MakeQualified(schemeWithAuthority
				, null), fsView.MakeQualified(new Path("/foo/bar")));
		}
	}
}
