using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestFcMainOperationsLocalFs : org.apache.hadoop.fs.FileContextMainOperationsBaseTest
	{
		internal org.apache.hadoop.fs.FileContext fclocal;

		internal org.apache.hadoop.fs.Path targetOfTests;

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

		protected internal override bool listCorruptedBlocksSupported()
		{
			return false;
		}
	}
}
