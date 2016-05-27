using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestFcMainOperationsLocalFs : FileContextMainOperationsBaseTest
	{
		internal FileContext fclocal;

		internal Path targetOfTests;

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

		protected internal override bool ListCorruptedBlocksSupported()
		{
			return false;
		}
	}
}
