using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestFcPermissionsLocalFs : FileContextPermissionBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
			ViewFsTestSetup.TearDownForViewFsLocalFs(fileContextTestHelper);
		}

		/// <exception cref="System.Exception"/>
		protected internal override FileContext GetFileContext()
		{
			return ViewFsTestSetup.SetupForViewFsLocalFs(fileContextTestHelper);
		}
	}
}
