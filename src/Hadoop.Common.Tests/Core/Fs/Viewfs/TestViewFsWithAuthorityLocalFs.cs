using NUnit.Framework;
using Org.Apache.Hadoop.FS;


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
	public class TestViewFsWithAuthorityLocalFs : ViewFsBaseTest
	{
		internal URI schemeWithAuthority;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			// create the test root on local_fs
			fcTarget = FileContext.GetLocalFSFileContext();
			base.SetUp();
			// this sets up conf (and fcView which we replace)
			// Now create a viewfs using a mount table called "default"
			// hence viewfs://default/
			schemeWithAuthority = new URI(FsConstants.ViewfsScheme, "default", "/", null, null
				);
			fcView = FileContext.GetFileContext(schemeWithAuthority, conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
		}

		[Fact]
		public override void TestBasicPaths()
		{
			Assert.Equal(schemeWithAuthority, fcView.GetDefaultFileSystem(
				).GetUri());
			Assert.Equal(fcView.MakeQualified(new Path("/user/" + Runtime.
				GetProperty("user.name"))), fcView.GetWorkingDirectory());
			Assert.Equal(fcView.MakeQualified(new Path("/user/" + Runtime.
				GetProperty("user.name"))), fcView.GetHomeDirectory());
			Assert.Equal(new Path("/foo/bar").MakeQualified(schemeWithAuthority
				, null), fcView.MakeQualified(new Path("/foo/bar")));
		}
	}
}
