using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestFcCreateMkdirLocalFs : FileContextCreateMkdirBaseTest
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			fc = ViewFsTestSetup.SetupForViewFsLocalFs(fileContextTestHelper);
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
			ViewFsTestSetup.TearDownForViewFsLocalFs(fileContextTestHelper);
		}
	}
}
