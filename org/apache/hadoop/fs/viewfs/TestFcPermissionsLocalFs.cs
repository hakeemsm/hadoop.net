using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestFcPermissionsLocalFs : org.apache.hadoop.fs.FileContextPermissionBase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			base.setUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void tearDown()
		{
			base.tearDown();
			org.apache.hadoop.fs.viewfs.ViewFsTestSetup.tearDownForViewFsLocalFs(fileContextTestHelper
				);
		}

		/// <exception cref="System.Exception"/>
		protected internal override org.apache.hadoop.fs.FileContext getFileContext()
		{
			return org.apache.hadoop.fs.viewfs.ViewFsTestSetup.setupForViewFsLocalFs(fileContextTestHelper
				);
		}
	}
}
