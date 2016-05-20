using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestFcCreateMkdirLocalFs : org.apache.hadoop.fs.FileContextCreateMkdirBaseTest
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			fc = org.apache.hadoop.fs.viewfs.ViewFsTestSetup.setupForViewFsLocalFs(fileContextTestHelper
				);
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
	}
}
