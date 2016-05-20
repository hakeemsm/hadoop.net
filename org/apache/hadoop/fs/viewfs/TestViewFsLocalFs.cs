using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestViewFsLocalFs : org.apache.hadoop.fs.viewfs.ViewFsBaseTest
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			// create the test root on local_fs
			fcTarget = org.apache.hadoop.fs.FileContext.getLocalFSFileContext();
			base.setUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void tearDown()
		{
			base.tearDown();
		}
	}
}
