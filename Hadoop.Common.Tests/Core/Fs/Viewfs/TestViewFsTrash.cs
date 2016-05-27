using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestViewFsTrash
	{
		internal FileSystem fsTarget;

		internal FileSystem fsView;

		internal Configuration conf;

		internal FileSystemTestHelper fileSystemTestHelper = new FileSystemTestHelper();

		internal class TestLFS : LocalFileSystem
		{
			internal Path home;

			/// <exception cref="System.IO.IOException"/>
			internal TestLFS(TestViewFsTrash _enclosing)
				: this(new Path(this._enclosing.fileSystemTestHelper.GetTestRootDir()))
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			internal TestLFS(TestViewFsTrash _enclosing, Path home)
				: base()
			{
				this._enclosing = _enclosing;
				// the target file system - the mount will point here
				this.home = home;
			}

			public override Path GetHomeDirectory()
			{
				return this.home;
			}

			private readonly TestViewFsTrash _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			fsTarget = FileSystem.GetLocal(new Configuration());
			fsTarget.Mkdirs(new Path(fileSystemTestHelper.GetTestRootPath(fsTarget), "dir1"));
			conf = ViewFileSystemTestSetup.CreateConfig();
			fsView = ViewFileSystemTestSetup.SetupForViewFileSystem(conf, fileSystemTestHelper
				, fsTarget);
			conf.Set("fs.defaultFS", FsConstants.ViewfsUri.ToString());
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
			ViewFileSystemTestSetup.TearDown(fileSystemTestHelper, fsTarget);
			fsTarget.Delete(new Path(fsTarget.GetHomeDirectory(), ".Trash/Current"), true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTrash()
		{
			TestTrash.TrashShell(conf, fileSystemTestHelper.GetTestRootPath(fsView), fsTarget
				, new Path(fsTarget.GetHomeDirectory(), ".Trash/Current"));
		}
	}
}
