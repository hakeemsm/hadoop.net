using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestFSMainOperationsLocalFileSystem : org.apache.hadoop.fs.FSMainOperationsBaseTest
	{
		internal org.apache.hadoop.fs.FileSystem fcTarget;

		/// <exception cref="System.Exception"/>
		protected internal override org.apache.hadoop.fs.FileSystem createFileSystem()
		{
			return org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup.setupForViewFileSystem
				(org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup.createConfig(), this, fcTarget
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			fcTarget = org.apache.hadoop.fs.FileSystem.getLocal(conf);
			base.setUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void tearDown()
		{
			base.tearDown();
			org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup.tearDown(this, fcTarget);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void testWDAbsolute()
		{
			org.apache.hadoop.fs.Path absoluteDir = getTestRootPath(fSys, "test/existingDir");
			fSys.mkdirs(absoluteDir);
			fSys.setWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.getWorkingDirectory());
		}
	}
}
