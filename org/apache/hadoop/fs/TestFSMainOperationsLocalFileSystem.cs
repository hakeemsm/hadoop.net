using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFSMainOperationsLocalFileSystem : org.apache.hadoop.fs.FSMainOperationsBaseTest
	{
		/// <exception cref="System.IO.IOException"/>
		protected internal override org.apache.hadoop.fs.FileSystem createFileSystem()
		{
			return org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void setUp()
		{
			base.setUp();
		}

		internal static org.apache.hadoop.fs.Path wd = null;

		/// <exception cref="System.IO.IOException"/>
		protected internal override org.apache.hadoop.fs.Path getDefaultWorkingDirectory(
			)
		{
			if (wd == null)
			{
				wd = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration
					()).getWorkingDirectory();
			}
			return wd;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void tearDown()
		{
			base.tearDown();
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
