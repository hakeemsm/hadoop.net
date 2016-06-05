using NUnit.Framework;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.FS
{
	public class TestFSMainOperationsLocalFileSystem : FSMainOperationsBaseTest
	{
		/// <exception cref="System.IO.IOException"/>
		protected internal override FileSystem CreateFileSystem()
		{
			return FileSystem.GetLocal(new Configuration());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		internal static Path wd = null;

		/// <exception cref="System.IO.IOException"/>
		protected internal override Path GetDefaultWorkingDirectory()
		{
			if (wd == null)
			{
				wd = FileSystem.GetLocal(new Configuration()).GetWorkingDirectory();
			}
			return wd;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public override void TestWDAbsolute()
		{
			Path absoluteDir = GetTestRootPath(fSys, "test/existingDir");
			fSys.Mkdirs(absoluteDir);
			fSys.SetWorkingDirectory(absoluteDir);
			Assert.Equal(absoluteDir, fSys.GetWorkingDirectory());
		}
	}
}
