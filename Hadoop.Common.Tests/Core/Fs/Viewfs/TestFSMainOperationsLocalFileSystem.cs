using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestFSMainOperationsLocalFileSystem : FSMainOperationsBaseTest
	{
		internal FileSystem fcTarget;

		/// <exception cref="System.Exception"/>
		protected internal override FileSystem CreateFileSystem()
		{
			return ViewFileSystemTestSetup.SetupForViewFileSystem(ViewFileSystemTestSetup.CreateConfig
				(), this, fcTarget);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			Configuration conf = new Configuration();
			fcTarget = FileSystem.GetLocal(conf);
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
			ViewFileSystemTestSetup.TearDown(this, fcTarget);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestWDAbsolute()
		{
			Path absoluteDir = GetTestRootPath(fSys, "test/existingDir");
			fSys.Mkdirs(absoluteDir);
			fSys.SetWorkingDirectory(absoluteDir);
			NUnit.Framework.Assert.AreEqual(absoluteDir, fSys.GetWorkingDirectory());
		}
	}
}
